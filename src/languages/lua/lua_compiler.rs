use super::lua_lexer::LuaLexer;
use super::lua_parsing::{parse_string, parse_unsigned_number};
use super::{LuaToken, LuaTokenLabel};
use crate::errors::{LuaCompilationError, SyntaxError};
use crate::interpreter::{
    Chunk, ConstantIndex, Instruction, Module, Primitive, Register, ReturnMode, SourceMapping,
};
use crate::FastHashMap;
use std::borrow::Cow;
use std::iter::Peekable;
use std::ops::Range;

const MAX_LOCALS: Register = 200;
// max is 255, but we want to reduce checks, and our functions use at most 4 registers per call
// so we can check for exceeding ~250 at the start. getting anywhere close to 200 seems unreasonable anyway
const REGISTER_LIMIT: Register = 250;
const FLUSH_TABLE_LIMIT: Register = 50;
const ENV_NAME: &str = "_ENV";

#[derive(Clone, Copy)]
enum VariablePath<'source> {
    /// Stack register
    Stack(Register),
    // Up value
    UpValue(Register),
    /// Two stack registers: table and key
    TableValue(LuaToken<'source>, Register, Register),
    /// Two stack registers: table and key
    TableField(LuaToken<'source>, Register, ConstantIndex),
    /// Stack register
    Result(Register),
}

#[derive(Default)]
pub struct LuaByteStrings<'source> {
    strings: Vec<Cow<'source, [u8]>>,
}

pub struct LuaByteStringIter<'source> {
    strings: LuaByteStrings<'source>,
}

impl<'source> Iterator for LuaByteStringIter<'source> {
    type Item = Cow<'source, [u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        self.strings.strings.pop()
    }
}

impl<'source> IntoIterator for LuaByteStrings<'source> {
    type Item = Cow<'source, [u8]>;
    type IntoIter = LuaByteStringIter<'source>;

    fn into_iter(mut self) -> Self::IntoIter {
        self.strings.reverse();

        LuaByteStringIter { strings: self }
    }
}

type CompilationOutput<'source> = Module<LuaByteStrings<'source>>;

#[derive(Default)]
struct Scope<'source> {
    first_register: Register,
    locals: FastHashMap<&'source str, Register>,
}

enum UpValueOrStack {
    UpValue(Register),
    Stack(Register),
}

struct Capture<'source> {
    token: LuaToken<'source>,
    parent: usize,
    parent_variable: UpValueOrStack,
}

#[derive(Default)]
struct FunctionContext<'source> {
    strings: LuaByteStrings<'source>,
    numbers: Vec<i64>,
    number_map: FastHashMap<i64, usize>,
    top_scope: Scope<'source>,
    scopes: Vec<Scope<'source>>,
    captures: Vec<Capture<'source>>,
    dependencies: Vec<usize>,
    instructions: Vec<Instruction>,
    source_map: Vec<SourceMapping>,
    accept_variadic: bool,
    named_param_count: Register,
    next_register: Register,
}

impl<'source> FunctionContext<'source> {
    fn push_scope(&mut self) {
        let old_scope = std::mem::take(&mut self.top_scope);
        self.top_scope.first_register = self.next_register;
        self.scopes.push(old_scope);
    }

    fn pop_scope(&mut self) {
        // recycle registers
        self.next_register = self.top_scope.first_register;
        self.top_scope = self.scopes.pop().unwrap();
    }

    fn register_number(
        &mut self,
        source: &'source str,
        token: LuaToken<'source>,
        number: i64,
    ) -> Result<ConstantIndex, LuaCompilationError> {
        let index = *self.number_map.entry(number).or_insert_with(|| {
            let index = self.numbers.len();
            self.numbers.push(number);
            index
        });

        if index > ConstantIndex::MAX as usize {
            return Err(LuaCompilationError::new_reached_number_limit(
                source,
                token.offset,
            ));
        }

        Ok(index as _)
    }

    fn register_up_value(
        &mut self,
        source: &'source str,
        token: LuaToken<'source>,
        parent: usize,
        parent_variable: UpValueOrStack,
    ) -> Result<Register, LuaCompilationError> {
        if self.captures.len() > Register::MAX as usize {
            return Err(LuaCompilationError::new_reached_capture_limit(
                source,
                token.offset,
            ));
        }

        let register = self.captures.len() as _;
        self.captures.push(Capture {
            token,
            parent,
            parent_variable,
        });

        Ok(register)
    }

    /// Registers a local on the stack, increments next_register
    fn register_local(
        &mut self,
        source: &'source str,
        token: LuaToken<'source>,
    ) -> Result<Register, LuaCompilationError> {
        if self.next_register >= MAX_LOCALS {
            return Err(LuaCompilationError::new_too_many_locals(
                source,
                token.offset,
            ));
        }

        let index = self.next_register;
        self.top_scope.locals.insert(token.content, index);
        self.next_register += 1;
        Ok(index)
    }

    fn registered_local(&self, name: &'source str) -> Option<Register> {
        if let Some(index) = self.top_scope.locals.get(name) {
            return Some(*index as _);
        }

        self.scopes
            .iter()
            .rev()
            .flat_map(|scope| scope.locals.get(name))
            .next()
            .cloned()
    }

    fn intern_string(
        &mut self,
        source: &'source str,
        token: LuaToken<'source>,
    ) -> Result<ConstantIndex, LuaCompilationError> {
        let bytes = parse_string(source, token)?;

        let strings = &mut self.strings.strings;

        if let Some(index) = strings.iter().position(|b| *b == bytes) {
            return Ok(index as _);
        }

        let index = strings.len();
        strings.push(bytes);
        Ok(index as _)
    }

    fn map_following_instructions(&mut self, source: &str, offset: usize) {
        // todo: improve performance by remembering the last offset and adding the last line and col?
        let mapping = SourceMapping::new(source, offset, self.instructions.len());
        self.source_map.push(mapping);
    }
}

#[derive(Default)]
pub struct LuaCompiler {
    lexer: LuaLexer,
}

impl LuaCompiler {
    pub fn compile<'source>(
        &self,
        source: &'source str,
    ) -> Result<CompilationOutput<'source>, LuaCompilationError> {
        CompilationJob::new(source, self.lexer.lex(source)).compile()
    }
}

struct CompilationJob<'source, I: Iterator> {
    source: &'source str,
    token_iter: Peekable<I>,
    top_function: FunctionContext<'source>,
    unresolved_breaks: Vec<(LuaToken<'source>, usize)>,
    function_stack: Vec<FunctionContext<'source>>,
    module: CompilationOutput<'source>,
}

impl<'source, I> CompilationJob<'source, I>
where
    I: Iterator<Item = Result<LuaToken<'source>, SyntaxError<LuaTokenLabel>>>,
{
    fn new(source: &'source str, token_iter: I) -> Self {
        Self {
            source,
            token_iter: token_iter.peekable(),
            top_function: FunctionContext::default(),
            unresolved_breaks: Default::default(),
            function_stack: Default::default(),
            module: Default::default(),
        }
    }

    fn compile(mut self) -> Result<CompilationOutput<'source>, LuaCompilationError> {
        // receive args at top level through variadic
        self.top_function.accept_variadic = true;
        // register the _ENV auto capture
        self.top_function.captures.push(Capture {
            token: LuaToken {
                label: LuaTokenLabel::Name,
                content: ENV_NAME,
                offset: 0,
            },
            parent: 1,
            parent_variable: UpValueOrStack::UpValue(0),
        });

        self.resolve_block()?;

        // catch unexpected breaks
        if let Some((token, _)) = self.unresolved_breaks.first() {
            return Err(LuaCompilationError::new_unexpected_break(
                self.source,
                token.offset,
            ));
        }

        // make sure we've exhausted all tokens
        if let Some(token) = self.token_iter.next().transpose()? {
            return Err(SyntaxError::new_unexpected_token(self.source, token).into());
        }

        // catch number limit
        if self.top_function.numbers.len() > ConstantIndex::MAX as usize {
            return Err(LuaCompilationError::new_reached_number_limit(
                self.source,
                self.source.len(),
            ));
        }

        self.module.byte_strings = self.top_function.strings;
        self.module.numbers = self.top_function.numbers;
        self.module.instructions = self.top_function.instructions;
        self.module.source_map = self.top_function.source_map;

        // catch number limit
        if self.module.chunks.len() > ConstantIndex::MAX as usize {
            return Err(LuaCompilationError::ReachedFunctionLimit);
        }

        Ok(self.module)
    }

    fn resolve_parameters(&mut self, implicit_count: u8) -> Result<(), LuaCompilationError> {
        self.expect(LuaTokenLabel::OpenParen)?;

        let mut named_count = implicit_count;

        loop {
            let token = self.expect_any()?;

            match token.label {
                LuaTokenLabel::Name => {
                    let local = self.top_function.register_local(self.source, token)?;
                    let instructions = &mut self.top_function.instructions;
                    instructions.push(Instruction::CopyArg(local, named_count));

                    named_count += 1;
                }
                LuaTokenLabel::TripleDot => {
                    self.top_function.accept_variadic = true;
                }
                LuaTokenLabel::CloseParen => break,
                _ => return Err(SyntaxError::new_unexpected_token(self.source, token).into()),
            }

            let token = self.expect_any()?;

            match token.label {
                LuaTokenLabel::Comma => {}
                LuaTokenLabel::CloseParen => break,
                _ => return Err(SyntaxError::new_unexpected_token(self.source, token).into()),
            }
        }

        // store count for resolving var args
        self.top_function.named_param_count = named_count;

        Ok(())
    }

    fn resolve_block(&mut self) -> Result<(), LuaCompilationError> {
        while let Some(res) = self.token_iter.peek().cloned() {
            let token = res?;

            match token.label {
                LuaTokenLabel::SemiColon => {
                    // consume token
                    self.token_iter.next();
                }
                LuaTokenLabel::Do => {
                    // consume token
                    self.token_iter.next();

                    self.top_function.push_scope();
                    self.resolve_block()?;
                    self.top_function.pop_scope();

                    self.expect(LuaTokenLabel::End)?;
                }
                LuaTokenLabel::If => {
                    // consume token
                    self.token_iter.next();

                    // todo: recycle?
                    let mut end_jumps = Vec::new();

                    loop {
                        let top_register = self.top_function.next_register;
                        let condition_register =
                            self.resolve_expression(top_register, ReturnMode::Static(1), 0)?;
                        self.expect(LuaTokenLabel::Then)?;

                        let instructions = &mut self.top_function.instructions;
                        instructions.push(Instruction::TestTruthy(false, condition_register));
                        let branch_index = instructions.len();
                        instructions.push(Instruction::Jump(0.into()));

                        self.top_function.push_scope();
                        self.resolve_block()?;
                        self.top_function.pop_scope();

                        let end_token = self.expect_any()?;

                        match end_token.label {
                            LuaTokenLabel::ElseIf => {
                                // insert + track end jump
                                let instructions = &mut self.top_function.instructions;
                                end_jumps.push(instructions.len());
                                instructions.push(Instruction::Jump(0.into()));

                                // update branch jump
                                let instruction_index = instructions.len().into();
                                instructions[branch_index] = Instruction::Jump(instruction_index);
                            }
                            LuaTokenLabel::Else => {
                                // insert + track end jump
                                let instructions = &mut self.top_function.instructions;
                                end_jumps.push(instructions.len());
                                instructions.push(Instruction::Jump(0.into()));

                                // update branch jump
                                let instruction_index = instructions.len().into();
                                instructions[branch_index] = Instruction::Jump(instruction_index);

                                self.top_function.push_scope();
                                self.resolve_block()?;
                                self.top_function.pop_scope();
                                self.expect(LuaTokenLabel::End)?;
                                break;
                            }
                            LuaTokenLabel::End => {
                                // update branch jump
                                let instructions = &mut self.top_function.instructions;
                                let instruction_index = instructions.len().into();
                                instructions[branch_index] = Instruction::Jump(instruction_index);
                                break;
                            }
                            _ => {
                                return Err(SyntaxError::new_unexpected_token(
                                    self.source,
                                    end_token,
                                )
                                .into())
                            }
                        }
                    }

                    let instructions = &mut self.top_function.instructions;
                    let end_index = instructions.len();

                    for index in end_jumps {
                        instructions[index] = Instruction::Jump(end_index.into());
                    }
                }
                LuaTokenLabel::While => {
                    // consume token
                    self.token_iter.next();

                    let instructions = &mut self.top_function.instructions;
                    let start_index = instructions.len();

                    let top_register = self.top_function.next_register;
                    let condition_register =
                        self.resolve_expression(top_register, ReturnMode::Static(1), 0)?;
                    self.expect(LuaTokenLabel::Do)?;

                    let instructions = &mut self.top_function.instructions;
                    instructions.push(Instruction::TestTruthy(false, condition_register));
                    let branch_index = instructions.len();
                    instructions.push(Instruction::Jump(0.into()));

                    self.top_function.push_scope();
                    self.resolve_block()?;
                    self.top_function.pop_scope();
                    self.expect(LuaTokenLabel::End)?;

                    let instructions = &mut self.top_function.instructions;
                    instructions.push(Instruction::Jump(start_index.into()));

                    // resolve break jumps
                    for (_, index) in &self.unresolved_breaks {
                        instructions[*index] = Instruction::Jump(instructions.len().into());
                    }
                    self.unresolved_breaks.clear();

                    // resolve branch jump
                    instructions[branch_index] = Instruction::Jump(instructions.len().into());
                }
                LuaTokenLabel::Repeat => {
                    // consume token
                    self.token_iter.next();

                    let instructions = &mut self.top_function.instructions;
                    let start_index = instructions.len();

                    self.top_function.push_scope();
                    self.resolve_block()?;
                    self.expect(LuaTokenLabel::Until)?;
                    // we'll pop scope later

                    // test to see if we need to jump back to the start
                    let top_register = self.top_function.next_register;
                    let condition_register =
                        self.resolve_expression(top_register, ReturnMode::Static(1), 0)?;
                    let instructions = &mut self.top_function.instructions;
                    instructions.push(Instruction::TestTruthy(false, condition_register));
                    instructions.push(Instruction::Jump(start_index.into()));

                    // resolve break jumps
                    for (_, index) in &self.unresolved_breaks {
                        instructions[*index] = Instruction::Jump(instructions.len().into());
                    }
                    self.unresolved_breaks.clear();

                    // pop the scope after resolving the expression
                    // the expression is part of the block oddly
                    self.top_function.pop_scope();
                }
                LuaTokenLabel::For => {
                    // consume token
                    self.token_iter.next();

                    self.top_function.push_scope();

                    let name_token = self.expect(LuaTokenLabel::Name)?;
                    let next_token = self.expect_any()?;

                    let mut use_for_jump = false;

                    let (start_index, branch_index) = match next_token.label {
                        LuaTokenLabel::Assign => {
                            use_for_jump = true;
                            self.resolve_numeric_for_params(name_token)?
                        }
                        LuaTokenLabel::Comma | LuaTokenLabel::In => {
                            self.resolve_generic_for_params(name_token, next_token)?
                        }
                        _ => {
                            return Err(
                                SyntaxError::new_unexpected_token(self.source, next_token).into()
                            )
                        }
                    };

                    self.test_register_limit(token, self.top_function.next_register)?;

                    self.expect(LuaTokenLabel::Do)?;
                    self.resolve_block()?;
                    self.top_function.pop_scope();
                    self.expect(LuaTokenLabel::End)?;

                    let instructions = &mut self.top_function.instructions;

                    // jump back to the start
                    if use_for_jump {
                        instructions.push(Instruction::JumpToForLoop(start_index.into()));
                    } else {
                        instructions.push(Instruction::Jump(start_index.into()));
                    }

                    // resolve break jumps
                    let next_instruction = instructions.len();

                    for (_, index) in &self.unresolved_breaks {
                        instructions[*index] = Instruction::Jump(next_instruction.into());
                    }
                    self.unresolved_breaks.clear();

                    // resolve branch jump
                    instructions[branch_index] = Instruction::Jump(next_instruction.into());
                }
                LuaTokenLabel::Break => {
                    // consume token
                    self.token_iter.next();

                    let instructions = &mut self.top_function.instructions;
                    self.unresolved_breaks.push((token, instructions.len()));
                    instructions.push(Instruction::Jump(0.into()));
                }
                LuaTokenLabel::Return => {
                    // consume token
                    self.token_iter.next();

                    let top_register = self.top_function.next_register;
                    self.resolve_exp_list(token, top_register)?;

                    let instructions = &mut self.top_function.instructions;
                    let mut optimized = false;

                    // tail call optimization
                    if let Some(Instruction::Call(register, return_mode)) = instructions.last_mut()
                    {
                        if *return_mode == ReturnMode::Extend(top_register)
                            && *register == top_register + 1
                        {
                            *return_mode = ReturnMode::TailCall;
                            optimized = true;
                        }
                    }

                    if !optimized {
                        instructions.push(Instruction::Return(top_register));
                    }

                    // check for semicolon
                    let next_token = self.token_iter.peek().cloned().transpose()?;

                    if next_token.is_some_and(|token| token.label == LuaTokenLabel::SemiColon) {
                        // consume semicolon
                        self.token_iter.next();
                    }
                    break;
                }
                LuaTokenLabel::Function => {
                    // consume token
                    self.token_iter.next();

                    let top_register = self.top_function.next_register;

                    let mut name_token = self.expect(LuaTokenLabel::Name)?;
                    let mut path = self.resolve_funcname_path(top_register, name_token)?;

                    // see if this is a method call by checking for a colon
                    let Some(next_token) = self.token_iter.peek().cloned().transpose()? else {
                        return Err(SyntaxError::UnexpectedEOF.into());
                    };

                    let is_method = next_token.label == LuaTokenLabel::Colon;

                    if is_method {
                        // consume colon token
                        self.token_iter.next();

                        // collapse value
                        self.copy_variable(top_register, path);

                        // store the string
                        name_token = self.expect(LuaTokenLabel::Name)?;

                        let string_index =
                            self.top_function.intern_string(self.source, name_token)?;

                        let instructions = &mut self.top_function.instructions;
                        instructions.push(Instruction::LoadBytes(top_register + 1, string_index));

                        // update path
                        path = VariablePath::TableValue(next_token, top_register, top_register + 1);
                    }

                    match path {
                        VariablePath::Stack(dest) => {
                            self.resolve_function_body(top_register, is_method)?;
                            let instructions = &mut self.top_function.instructions;
                            instructions.push(Instruction::CopyToDeref(dest, top_register));
                        }
                        VariablePath::UpValue(dest) => {
                            self.resolve_function_body(top_register, is_method)?;
                            let instructions = &mut self.top_function.instructions;
                            instructions.push(Instruction::CopyToUpValueDeref(dest, top_register));
                        }
                        VariablePath::TableValue(token, table_index, key_index) => {
                            self.resolve_function_body(top_register + 2, is_method)?;

                            self.top_function
                                .map_following_instructions(self.source, token.offset);

                            let instructions = &mut self.top_function.instructions;
                            instructions.push(Instruction::CopyToTableValue(
                                table_index,
                                key_index,
                                top_register + 2,
                            ));
                        }
                        VariablePath::TableField(token, table_index, string_index) => {
                            self.resolve_function_body(top_register + 1, is_method)?;

                            self.top_function
                                .map_following_instructions(self.source, token.offset);

                            let instructions = &mut self.top_function.instructions;
                            instructions
                                .push(Instruction::CopyToTableField(table_index, top_register + 1));
                            instructions.push(Instruction::Constant(string_index));
                        }
                        VariablePath::Result(_) => unreachable!(),
                    }
                }
                LuaTokenLabel::Local => {
                    // consume token
                    self.token_iter.next();

                    let mut peeked_token = self.token_iter.peek().cloned().transpose()?;

                    if peeked_token.is_some_and(|token| token.label == LuaTokenLabel::Function) {
                        // consume token
                        self.token_iter.next();

                        let name_token = self.expect(LuaTokenLabel::Name)?;
                        let local = self.top_function.register_local(self.source, name_token)?;

                        // store nil since the function may try to refer to itself and we don't want it promoting + using an old value
                        let instructions = &mut self.top_function.instructions;
                        instructions.push(Instruction::SetNil(local));

                        let top_register = self.top_function.next_register;
                        self.resolve_function_body(top_register, false)?;

                        self.copy_stack_value(local, top_register);
                    } else {
                        let name_token = self.expect(LuaTokenLabel::Name)?;

                        // todo: maybe recycle this?
                        let mut lhs_list = vec![name_token];

                        loop {
                            peeked_token = self.token_iter.peek().cloned().transpose()?;

                            let Some(token) = peeked_token else {
                                break;
                            };

                            // todo: handle attributes?

                            if token.label != LuaTokenLabel::Comma {
                                break;
                            }

                            // consume comma token
                            self.token_iter.next();

                            let name_token = self.expect(LuaTokenLabel::Name)?;
                            lhs_list.push(name_token);
                        }

                        let first_lhs_index = self.top_function.next_register;

                        if peeked_token.is_some_and(|token| token.label == LuaTokenLabel::Assign) {
                            // consume token
                            self.token_iter.next();

                            self.resolve_unsized_exp_list(first_lhs_index)?;
                        } else {
                            let instructions = &mut self.top_function.instructions;

                            for i in 0..(lhs_list.len() as Register) {
                                instructions.push(Instruction::SetNil(i + first_lhs_index));
                            }
                        }

                        for token in lhs_list {
                            self.top_function.register_local(self.source, token)?;
                        }
                    }
                }
                LuaTokenLabel::OpenParen | LuaTokenLabel::Name => {
                    // consume token
                    self.token_iter.next();

                    let top_register = self.top_function.next_register;
                    let path =
                        self.resolve_variable_path(top_register, token, ReturnMode::Static(0))?;

                    if matches!(path, VariablePath::Result(_)) {
                        continue;
                    }
                    let next_token = self.expect_any()?;

                    if next_token.label == LuaTokenLabel::Comma {
                        // todo: maybe recycle this?
                        let mut lhs_list = vec![path];

                        let mut next_register = self.top_function.next_register;

                        match path {
                            VariablePath::TableValue(..) => next_register += 2,
                            VariablePath::TableField(..) => next_register += 1,
                            _ => {}
                        }

                        loop {
                            let variable_start_token = self.expect_any()?;
                            let path = self.resolve_variable_path(
                                next_register,
                                variable_start_token,
                                ReturnMode::Static(0),
                            )?;

                            if matches!(path, VariablePath::Result(_)) {
                                // we need to be able to write to the variable
                                return Err(SyntaxError::new_unexpected_token(
                                    self.source,
                                    variable_start_token,
                                )
                                .into());
                            }

                            match path {
                                VariablePath::TableValue(..) => next_register += 2,
                                VariablePath::TableField(..) => next_register += 1,
                                _ => {}
                            }

                            lhs_list.push(path);

                            let Some(token) = self.token_iter.peek().cloned().transpose()? else {
                                break;
                            };

                            if token.label != LuaTokenLabel::Comma {
                                break;
                            }

                            // consume comma token
                            self.token_iter.next();
                        }

                        let assign_token = self.expect(LuaTokenLabel::Assign)?;

                        self.resolve_exp_list(assign_token, next_register)?;

                        for path in lhs_list {
                            // start with increment to skip the len
                            next_register += 1;

                            match path {
                                VariablePath::Stack(dest) => {
                                    let instructions = &mut self.top_function.instructions;
                                    instructions
                                        .push(Instruction::CopyToDeref(dest, next_register));
                                }
                                VariablePath::UpValue(dest) => {
                                    let instructions = &mut self.top_function.instructions;
                                    instructions
                                        .push(Instruction::CopyToUpValueDeref(dest, next_register));
                                }
                                VariablePath::TableValue(token, table_index, key_index) => {
                                    self.top_function
                                        .map_following_instructions(self.source, token.offset);

                                    let instructions = &mut self.top_function.instructions;
                                    instructions.push(Instruction::CopyToTableValue(
                                        table_index,
                                        key_index,
                                        next_register,
                                    ));
                                }
                                VariablePath::TableField(token, table_index, string_index) => {
                                    self.top_function
                                        .map_following_instructions(self.source, token.offset);

                                    let instructions = &mut self.top_function.instructions;
                                    instructions.push(Instruction::CopyToTableField(
                                        table_index,
                                        next_register,
                                    ));
                                    instructions.push(Instruction::Constant(string_index));
                                }
                                VariablePath::Result(_) => unreachable!(),
                            }
                        }
                    } else if next_token.label == LuaTokenLabel::Assign {
                        let top_register = self.top_function.next_register;

                        match path {
                            VariablePath::Stack(dest) => {
                                let value_register = self.resolve_expression(
                                    top_register + 1,
                                    ReturnMode::Static(1),
                                    0,
                                )?;

                                let instructions = &mut self.top_function.instructions;
                                instructions.push(Instruction::CopyToDeref(dest, value_register));
                            }
                            VariablePath::UpValue(dest) => {
                                let value_register = self.resolve_expression(
                                    top_register + 1,
                                    ReturnMode::Static(1),
                                    0,
                                )?;

                                let instructions = &mut self.top_function.instructions;
                                instructions
                                    .push(Instruction::CopyToUpValueDeref(dest, value_register));
                            }
                            VariablePath::TableValue(token, table_index, key_index) => {
                                let value_register = self.resolve_expression(
                                    top_register + 2,
                                    ReturnMode::Static(1),
                                    0,
                                )?;

                                self.top_function
                                    .map_following_instructions(self.source, token.offset);

                                let instructions = &mut self.top_function.instructions;
                                instructions.push(Instruction::CopyToTableValue(
                                    table_index,
                                    key_index,
                                    value_register,
                                ));
                            }
                            VariablePath::TableField(token, table_index, string_index) => {
                                let value_register = self.resolve_expression(
                                    top_register + 1,
                                    ReturnMode::Static(1),
                                    0,
                                )?;

                                self.top_function
                                    .map_following_instructions(self.source, token.offset);

                                let instructions = &mut self.top_function.instructions;
                                instructions.push(Instruction::CopyToTableField(
                                    table_index,
                                    value_register,
                                ));
                                instructions.push(Instruction::Constant(string_index));
                            }
                            VariablePath::Result(_) => unreachable!(),
                        }

                        if let Some(token) = self.token_iter.peek().cloned().transpose()? {
                            if token.label == LuaTokenLabel::Comma {
                                // consume token
                                self.token_iter.next();

                                let top_register = self.top_function.next_register;
                                self.resolve_exp_list(token, top_register)?;
                            }
                        }
                    }
                }

                _ => break,
            }
        }

        Ok(())
    }

    fn expect(&mut self, label: LuaTokenLabel) -> Result<LuaToken<'source>, LuaCompilationError> {
        let token = self.expect_any()?;

        if token.label != label {
            return Err(SyntaxError::new_unexpected_token(self.source, token).into());
        }

        Ok(token)
    }

    fn expect_any(&mut self) -> Result<LuaToken<'source>, LuaCompilationError> {
        self.token_iter
            .next()
            .transpose()?
            .ok_or(SyntaxError::UnexpectedEOF.into())
    }

    fn test_register_limit(
        &self,
        token: LuaToken,
        top_register: Register,
    ) -> Result<(), LuaCompilationError> {
        if top_register > REGISTER_LIMIT {
            return Err(LuaCompilationError::new_reached_register_limit(
                self.source,
                token.offset,
            ));
        }
        Ok(())
    }

    /// return_mode_hint is a hint since it's only used if the last piece of the variable is a function call
    /// in all other cases it's ReturnMode::Static(1)
    ///
    /// if the final path is VariablePath::Result, assume it's a function call
    fn resolve_variable_path(
        &mut self,
        top_register: Register,
        token: LuaToken<'source>,
        return_mode_hint: ReturnMode,
    ) -> Result<VariablePath<'source>, LuaCompilationError> {
        let mut path = match token.label {
            LuaTokenLabel::Name => self.resolve_name_path(top_register, token)?,
            LuaTokenLabel::OpenParen => {
                let register = self.resolve_expression(top_register, ReturnMode::Static(1), 0)?;
                self.expect(LuaTokenLabel::CloseParen)?;

                VariablePath::Result(register)
            }
            _ => return Err(SyntaxError::new_unexpected_token(self.source, token).into()),
        };

        loop {
            let Some(token) = self.token_iter.peek().cloned().transpose()? else {
                break;
            };

            match token.label {
                LuaTokenLabel::Dot => {
                    let table_register = self.collapse_variable(top_register, path);

                    // consume dot
                    self.token_iter.next();

                    let token = self.expect(LuaTokenLabel::Name)?;

                    let string_index = self.top_function.intern_string(self.source, token)?;
                    path = VariablePath::TableField(token, table_register, string_index);
                }
                LuaTokenLabel::OpenBracket => {
                    let table_register = self.collapse_variable(top_register, path);

                    // consume bracket
                    self.token_iter.next();

                    let key_register =
                        self.resolve_expression(top_register + 1, ReturnMode::Static(1), 0)?;

                    self.expect(LuaTokenLabel::CloseBracket)?;

                    path = VariablePath::TableValue(token, table_register, key_register);
                }
                _ if starts_function_call(token.label) => {
                    self.copy_variable(top_register, path);

                    // consume token
                    self.token_iter.next();

                    self.resolve_function_call(token, top_register, return_mode_hint)?;

                    path = VariablePath::Result(top_register);
                }
                _ => break,
            }
        }

        Ok(path)
    }

    /// expects the token to be a name token
    /// stops before the colon
    fn resolve_funcname_path(
        &mut self,
        top_register: Register,
        token: LuaToken<'source>,
    ) -> Result<VariablePath<'source>, LuaCompilationError> {
        let mut path = self.resolve_name_path(top_register, token)?;

        loop {
            let Some(separator_token) = self.token_iter.peek().cloned().transpose()? else {
                break;
            };

            if separator_token.label != LuaTokenLabel::Dot {
                break;
            }

            self.copy_variable(top_register, path);

            // consume dot
            self.token_iter.next();

            let token = self.expect(LuaTokenLabel::Name)?;
            let string_index = self.top_function.intern_string(self.source, token)?;

            path = VariablePath::TableField(token, top_register, string_index);
        }

        Ok(path)
    }

    /// expects the token to be a name token
    /// never returns VariablePath::Result
    fn resolve_name_path(
        &mut self,
        top_register: Register,
        token: LuaToken<'source>,
    ) -> Result<VariablePath<'source>, LuaCompilationError> {
        let path = if let Some(index) = self.top_function.registered_local(token.content) {
            VariablePath::Stack(index)
        } else if let Some(index) = self.try_capture(token)? {
            // capture
            VariablePath::UpValue(index)
        } else {
            let string_index = self.top_function.intern_string(self.source, token)?;

            if let Some(local) = self.top_function.registered_local(ENV_NAME) {
                // treat as environment local
                VariablePath::TableField(token, local, string_index)
            } else {
                // capture environment
                let up_value_index = self
                    .try_capture(LuaToken {
                        label: LuaTokenLabel::Name,
                        content: ENV_NAME,
                        offset: token.offset,
                    })?
                    .unwrap();

                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::CopyUpValue(top_register, up_value_index));

                VariablePath::TableField(token, top_register, string_index)
            }
        };

        Ok(path)
    }

    fn try_capture(
        &mut self,
        token: LuaToken<'source>,
    ) -> Result<Option<Register>, LuaCompilationError> {
        // not expecting many captures, so just iterating
        for (i, capture) in self.top_function.captures.iter().enumerate() {
            if capture.token.content == token.content {
                return Ok(Some(i as _));
            }
        }

        for (i, function) in self.function_stack.iter().rev().enumerate() {
            if let Some(&register) = function.top_scope.locals.get(token.content) {
                return Ok(Some(self.top_function.register_up_value(
                    self.source,
                    token,
                    i,
                    UpValueOrStack::Stack(register),
                )?));
            }

            for scope in &function.scopes {
                if let Some(&register) = scope.locals.get(token.content) {
                    return Ok(Some(self.top_function.register_up_value(
                        self.source,
                        token,
                        i,
                        UpValueOrStack::Stack(register),
                    )?));
                }
            }

            for (j, capture) in function.captures.iter().enumerate() {
                if capture.token.content == token.content {
                    return Ok(Some(self.top_function.register_up_value(
                        self.source,
                        token,
                        i,
                        UpValueOrStack::UpValue(j as _),
                    )?));
                }
            }
        }

        Ok(None)
    }

    fn copy_variable(&mut self, dest: Register, path: VariablePath) {
        match path {
            VariablePath::Stack(src) => {
                self.copy_stack_value(dest, src);
            }
            VariablePath::UpValue(src) => {
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::CopyUpValue(dest, src));
            }
            VariablePath::TableValue(token, table_index, key_index) => {
                self.top_function
                    .map_following_instructions(self.source, token.offset);

                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::CopyTableValue(dest, table_index, key_index));
            }
            VariablePath::TableField(token, table_index, string_index) => {
                self.top_function
                    .map_following_instructions(self.source, token.offset);

                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::CopyTableField(dest, table_index));
                instructions.push(Instruction::Constant(string_index));
            }
            VariablePath::Result(src) => {
                self.copy_stack_value(dest, src);
            }
        };
    }

    fn collapse_variable(&mut self, temp_register: Register, path: VariablePath) -> Register {
        match path {
            VariablePath::Stack(src) => src,
            VariablePath::UpValue(src) => {
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::CopyUpValue(temp_register, src));
                temp_register
            }
            VariablePath::TableValue(token, table_index, key_index) => {
                self.top_function
                    .map_following_instructions(self.source, token.offset);

                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::CopyTableValue(
                    temp_register,
                    table_index,
                    key_index,
                ));
                temp_register
            }
            VariablePath::TableField(token, table_index, string_index) => {
                self.top_function
                    .map_following_instructions(self.source, token.offset);

                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::CopyTableField(temp_register, table_index));
                instructions.push(Instruction::Constant(string_index));
                temp_register
            }
            VariablePath::Result(src) => src,
        }
    }

    fn copy_stack_value(&mut self, dest: Register, src: Register) {
        if dest != src {
            let instructions = &mut self.top_function.instructions;
            instructions.push(Instruction::CopyToDeref(dest, src));
        }
    }

    fn resolve_function_body(
        &mut self,
        top_register: Register,
        register_self: bool,
    ) -> Result<(), LuaCompilationError> {
        // swap top function
        let old_top_function = std::mem::take(&mut self.top_function);
        self.function_stack.push(old_top_function);

        let mut implicit_param_count = 0;

        if register_self {
            let local = self
                .top_function
                .register_local(
                    self.source,
                    LuaToken {
                        content: "self",
                        label: LuaTokenLabel::Name,
                        offset: 0,
                    },
                )
                .expect("this should be the second local, and we should have room for more");

            let instructions = &mut self.top_function.instructions;
            instructions.push(Instruction::CopyArg(local, 0));

            implicit_param_count = 1;
        }

        self.resolve_parameters(implicit_param_count)?;
        self.resolve_block()?;
        let token = self.expect(LuaTokenLabel::End)?;

        // catch number limit
        if self.top_function.numbers.len() > ConstantIndex::MAX as usize {
            return Err(LuaCompilationError::new_reached_number_limit(
                self.source,
                token.offset,
            ));
        }

        // catch unexpected breaks
        if let Some((token, _)) = self.unresolved_breaks.first() {
            return Err(LuaCompilationError::new_unexpected_break(
                self.source,
                token.offset,
            ));
        }

        // swap back top function
        let mut function = self.function_stack.pop().unwrap();
        std::mem::swap(&mut function, &mut self.top_function);

        // resolve captures
        for (dest, capture) in function.captures.into_iter().enumerate() {
            let up_value_or_stack = if capture.parent == 0 {
                capture.parent_variable
            } else {
                UpValueOrStack::UpValue(self.top_function.register_up_value(
                    self.source,
                    capture.token,
                    capture.parent - 1,
                    capture.parent_variable,
                )?)
            };

            let instructions = &mut self.top_function.instructions;
            let instruction = match up_value_or_stack {
                UpValueOrStack::UpValue(src) => Instruction::CaptureUpValue(dest as _, src),
                UpValueOrStack::Stack(src) => Instruction::Capture(dest as _, src),
            };
            instructions.push(instruction);
        }

        // store our processed function in the module
        let chunk_index = self.module.chunks.len();
        self.module.chunks.push(Chunk {
            byte_strings: function.strings,
            numbers: function.numbers,
            dependencies: function.dependencies,
            instructions: function.instructions,
            source_map: function.source_map,
        });

        // track dependency
        let function_index = if self.function_stack.is_empty() {
            // separate mode for the root function as it has no dependencies list and implicitly depends on every function
            chunk_index
        } else {
            self.top_function.dependencies.push(chunk_index);
            self.top_function.dependencies.len() - 1
        };

        // create closure
        let instructions = &mut self.top_function.instructions;
        instructions.push(Instruction::Closure(top_register, function_index as _));

        Ok(())
    }

    fn resolve_function_call(
        &mut self,
        token: LuaToken<'source>,
        top_register: Register,
        return_mode: ReturnMode,
    ) -> Result<(), LuaCompilationError> {
        let call_instruction_index;

        match token.label {
            LuaTokenLabel::OpenParen => {
                self.resolve_exp_list(token, top_register + 1)?;
                self.expect(LuaTokenLabel::CloseParen)?;

                // map the call
                self.top_function
                    .map_following_instructions(self.source, token.offset);

                let instructions = &mut self.top_function.instructions;
                call_instruction_index = instructions.len();
                instructions.push(Instruction::Call(top_register, return_mode));
            }

            LuaTokenLabel::StringLiteral => {
                // map the call
                self.top_function
                    .map_following_instructions(self.source, token.offset);

                // load a single string
                let string_index = self.top_function.intern_string(self.source, token)?;

                let count_constant = self.top_function.register_number(self.source, token, 1)?;
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::LoadInt(top_register + 1, count_constant));
                instructions.push(Instruction::LoadBytes(top_register + 2, string_index));
                call_instruction_index = instructions.len();
                instructions.push(Instruction::Call(top_register, return_mode));
            }

            LuaTokenLabel::OpenCurly => {
                self.resolve_table(top_register + 2)?;

                // map the call
                self.top_function
                    .map_following_instructions(self.source, token.offset);

                let count_constant = self.top_function.register_number(self.source, token, 1)?;
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::LoadInt(top_register + 1, count_constant));
                call_instruction_index = instructions.len();
                instructions.push(Instruction::Call(top_register, return_mode));
            }

            LuaTokenLabel::Colon => {
                let name_token = self.expect(LuaTokenLabel::Name)?;
                let string_index = self.top_function.intern_string(self.source, name_token)?;

                let next_token = self.expect_any()?;

                match next_token.label {
                    LuaTokenLabel::OpenParen => {
                        self.resolve_exp_list(next_token, top_register + 2)?;

                        let count_constant =
                            self.top_function
                                .register_number(self.source, next_token, 1)?;
                        let instructions = &mut self.top_function.instructions;
                        // increment the arg count and move it down
                        instructions.push(Instruction::LoadInt(top_register + 1, count_constant));
                        instructions.push(Instruction::Add(
                            top_register + 1,
                            top_register + 1,
                            top_register + 2,
                        ));
                        // copy the table into args
                        instructions.push(Instruction::Copy(top_register + 2, top_register));

                        self.expect(LuaTokenLabel::CloseParen)?;
                    }
                    LuaTokenLabel::StringLiteral => {
                        let string_index =
                            self.top_function.intern_string(self.source, name_token)?;

                        let count_constant =
                            self.top_function
                                .register_number(self.source, next_token, 2)?;
                        let instructions = &mut self.top_function.instructions;
                        instructions.push(Instruction::LoadInt(top_register + 1, count_constant));
                        instructions.push(Instruction::Copy(top_register + 2, top_register));
                        instructions.push(Instruction::LoadBytes(top_register + 3, string_index));
                    }
                    LuaTokenLabel::OpenCurly => {
                        let count_constant =
                            self.top_function
                                .register_number(self.source, next_token, 2)?;

                        let instructions = &mut self.top_function.instructions;
                        instructions.push(Instruction::LoadInt(top_register + 1, count_constant));
                        instructions.push(Instruction::Copy(top_register + 2, top_register));

                        self.resolve_table(top_register + 3)?;
                    }

                    _ => {
                        return Err(
                            SyntaxError::new_unexpected_token(self.source, next_token).into()
                        );
                    }
                }

                self.top_function
                    .map_following_instructions(self.source, next_token.offset);

                let instructions = &mut self.top_function.instructions;
                // load function
                instructions.push(Instruction::CopyTableField(top_register, top_register + 2));
                instructions.push(Instruction::Constant(string_index));
                // call function
                call_instruction_index = instructions.len();
                instructions.push(Instruction::Call(top_register, return_mode));
            }

            _ => {
                return Err(SyntaxError::new_unexpected_token(self.source, token).into());
            }
        }

        let Some(next_token) = self.token_iter.peek().cloned().transpose()? else {
            return Ok(());
        };

        if starts_function_call(next_token.label)
            || binary_operator_priority(next_token.label).0 > 0
            || continues_variable_path(next_token.label)
            || next_token.label == LuaTokenLabel::Comma
        {
            // rewrite the function call to have just one result
            let instructions = &mut self.top_function.instructions;
            instructions[call_instruction_index] =
                Instruction::Call(top_register, ReturnMode::Static(1));
        }

        Ok(())
    }

    /// Adds the count followed by expression results to the stack
    fn resolve_exp_list(
        &mut self,
        first_token: LuaToken<'source>,
        top_register: Register,
    ) -> Result<(), LuaCompilationError> {
        let mut total = 0;

        // initialize the argument count, we'll update it later
        let count_constant = self
            .top_function
            .register_number(self.source, first_token, 0)?;
        let instructions = &mut self.top_function.instructions;
        let count_instruction_index = instructions.len();
        instructions.push(Instruction::PrepMulti(top_register, count_constant));

        let Some(next_token) = self.token_iter.peek().cloned().transpose()? else {
            return Ok(());
        };

        if !starts_expression(next_token.label) {
            return Ok(());
        }

        // track the start of the first and last expression for updating function return modes to variadic
        let first_expression_start = instructions.len();
        let mut last_expression_start = instructions.len();

        // resolve the first expression
        let r = self.resolve_expression(top_register + 1, ReturnMode::Extend(top_register), 0)?;
        self.copy_stack_value(top_register + 1, r);
        total += 1;

        // resolve the rest
        while let Some(next_token) = self.token_iter.peek().cloned().transpose()? {
            if next_token.label != LuaTokenLabel::Comma {
                break;
            }

            // consume comma
            self.token_iter.next();

            let instructions = &mut self.top_function.instructions;
            last_expression_start = instructions.len();

            let dest = top_register + 1 + total;
            let r = self.resolve_expression(dest, ReturnMode::Extend(top_register), 0)?;
            self.copy_stack_value(dest, r);
            total += 1;
        }

        // update the count
        let instructions = &mut self.top_function.instructions;

        if matches!(instructions.last(), Some(Instruction::CopyVariadic(..))) {
            // let the variadic update this
            total -= 1;
        }

        let count_constant =
            self.top_function
                .register_number(self.source, next_token, total as _)?;
        let instructions = &mut self.top_function.instructions;
        instructions[count_instruction_index] =
            Instruction::PrepMulti(top_register, count_constant);

        self.variadic_to_single_static(top_register, first_expression_start..last_expression_start);

        Ok(())
    }

    /// Adds expression results to the stack without a count
    fn resolve_unsized_exp_list(
        &mut self,
        top_register: Register,
    ) -> Result<(), LuaCompilationError> {
        let instructions = &mut self.top_function.instructions;
        instructions.push(Instruction::ClearAfter(top_register));

        let Some(next_token) = self.token_iter.peek().cloned().transpose()? else {
            return Ok(());
        };

        if !starts_expression(next_token.label) {
            return Ok(());
        }

        // track the start of the first and last expression for updating function return modes to variadic
        let first_expression_start = instructions.len();
        let mut last_expression_start = instructions.len();

        // resolve the first expression
        let r = self.resolve_expression(
            top_register,
            ReturnMode::UnsizedDestinationPreserve(top_register),
            0,
        )?;
        self.copy_stack_value(top_register, r);
        let mut i = 1;

        // resolve the rest
        while let Some(next_token) = self.token_iter.peek().cloned().transpose()? {
            if next_token.label != LuaTokenLabel::Comma {
                break;
            }

            // consume comma
            self.token_iter.next();

            let instructions = &mut self.top_function.instructions;
            last_expression_start = instructions.len();

            let dest = top_register + i;
            let r =
                self.resolve_expression(dest, ReturnMode::UnsizedDestinationPreserve(dest), 0)?;
            self.copy_stack_value(dest, r);
            i += 1;
        }

        self.variadic_to_single_static(top_register, first_expression_start..last_expression_start);

        Ok(())
    }

    /// Changes instructions in `range`:
    ///  - Replaces Variadic return modes targeting a matching count_register will be replaced Static(1)
    ///  - Replaces CopyVariadic instructions matching the target count_register to a single CopyArg
    fn variadic_to_single_static(&mut self, count_register: Register, range: Range<usize>) {
        let instructions = &mut self.top_function.instructions;

        for instruction in &mut instructions[range] {
            match instruction {
                Instruction::Call(_, mode) => {
                    // for ReturnMode::Extend we need to test the target in case of a nested exp_list
                    // for ReturnMode::UnsizedDestinationPreserve we don't need to test the target,
                    // since it's only used in assignment, which can't be nested
                    if *mode == ReturnMode::Extend(count_register)
                        || matches!(mode, ReturnMode::UnsizedDestinationPreserve(_))
                    {
                        *mode = ReturnMode::Static(1);
                    }
                }
                Instruction::CopyVariadic(dest, count_dest, skip) => {
                    // need to test the target in case of nested exp_list
                    if *count_dest == count_register {
                        *instruction = Instruction::CopyArg(*dest, *skip);
                    }
                }
                _ => {}
            }
        }
    }

    /// Returns the register that stores the result
    /// Differs from top_register if the expression resolves to a variable
    fn resolve_expression(
        &mut self,
        top_register: Register,
        return_mode: ReturnMode,
        operation_priority: u8,
    ) -> Result<Register, LuaCompilationError> {
        let token = self.expect_any()?;
        self.resolve_partially_consumed_expression(
            top_register,
            return_mode,
            token,
            operation_priority,
        )
    }

    /// Returns the register that stores the result
    /// Differs from top_register if the expression resolves to a variable
    fn resolve_partially_consumed_expression(
        &mut self,
        top_register: Register,
        return_mode: ReturnMode,
        token: LuaToken<'source>,
        operation_priority: u8,
    ) -> Result<Register, LuaCompilationError> {
        self.test_register_limit(token, top_register)?;

        let mut result_register = top_register;
        let result_register = &mut result_register;

        match token.label {
            LuaTokenLabel::Nil => {
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::SetNil(top_register));
                while self.resolve_operation(top_register, result_register, operation_priority)? {}
            }
            LuaTokenLabel::True => {
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::SetBool(top_register, true));
                while self.resolve_operation(top_register, result_register, operation_priority)? {}
            }
            LuaTokenLabel::False => {
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::SetBool(top_register, false));
                while self.resolve_operation(top_register, result_register, operation_priority)? {}
            }
            LuaTokenLabel::Numeral => {
                let instruction = match parse_unsigned_number(token.content) {
                    Primitive::Integer(i) => {
                        let constant = self.top_function.register_number(self.source, token, i)?;
                        Instruction::LoadInt(top_register, constant)
                    }
                    Primitive::Float(f) => {
                        let i = f.to_bits() as i64;
                        let constant = self.top_function.register_number(self.source, token, i)?;
                        Instruction::LoadFloat(top_register, constant)
                    }
                    _ => {
                        return Err(LuaCompilationError::new_invalid_number(
                            self.source,
                            token.offset,
                        ))
                    }
                };
                let instructions = &mut self.top_function.instructions;
                instructions.push(instruction);
                while self.resolve_operation(top_register, result_register, operation_priority)? {}
            }
            LuaTokenLabel::StringLiteral => {
                let string_index = self.top_function.intern_string(self.source, token)?;
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::LoadBytes(top_register, string_index));
                while self.resolve_operation(top_register, result_register, operation_priority)? {}
            }
            LuaTokenLabel::OpenCurly => {
                self.resolve_table(top_register)?;
                while self.resolve_operation(top_register, result_register, operation_priority)? {}
            }
            LuaTokenLabel::Hash => {
                // len
                let register =
                    self.resolve_expression(top_register, return_mode, unary_priority())?;

                self.top_function
                    .map_following_instructions(self.source, token.offset);

                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::Len(top_register, register));

                while self.resolve_operation(top_register, result_register, operation_priority)? {}
            }
            LuaTokenLabel::Minus => {
                // unary minus
                *result_register =
                    self.resolve_expression(top_register, return_mode, unary_priority())?;
                while self.resolve_operation(top_register, result_register, unary_priority())? {}
                while self.resolve_operation(top_register, result_register, operation_priority)? {}

                self.top_function
                    .map_following_instructions(self.source, token.offset);

                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::UnaryMinus(top_register, *result_register));
                *result_register = top_register;
            }
            LuaTokenLabel::Not => {
                // not
                *result_register =
                    self.resolve_expression(top_register, return_mode, unary_priority())?;
                while self.resolve_operation(top_register, result_register, unary_priority())? {}
                while self.resolve_operation(top_register, result_register, operation_priority)? {}

                self.top_function
                    .map_following_instructions(self.source, token.offset);

                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::Not(top_register, *result_register));
                *result_register = top_register;
            }
            LuaTokenLabel::Tilde => {
                // bitwise not
                *result_register =
                    self.resolve_expression(top_register, return_mode, unary_priority())?;
                while self.resolve_operation(top_register, result_register, unary_priority())? {}
                while self.resolve_operation(top_register, result_register, operation_priority)? {}

                self.top_function
                    .map_following_instructions(self.source, token.offset);

                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::BitwiseNot(top_register, *result_register));
                *result_register = top_register;
            }
            LuaTokenLabel::OpenParen | LuaTokenLabel::Name => {
                // variable or function
                let path = self.resolve_variable_path(top_register, token, return_mode)?;
                *result_register = self.collapse_variable(top_register, path);

                while self.resolve_operation(top_register, result_register, operation_priority)? {}
            }
            LuaTokenLabel::Function => {
                self.resolve_function_body(top_register, false)?;
                while self.resolve_operation(top_register, result_register, operation_priority)? {}
            }
            LuaTokenLabel::TripleDot => {
                // variadic

                if !self.top_function.accept_variadic {
                    return Err(LuaCompilationError::new_unexpected_variadic(
                        self.source,
                        token.offset,
                    ));
                }

                let skip = self.top_function.named_param_count;
                let followed_by_operator = self
                    .token_iter
                    .peek()
                    .cloned()
                    .transpose()?
                    .is_some_and(|token| binary_operator_priority(token.label).0 > 0);

                let instructions = &mut self.top_function.instructions;

                if followed_by_operator {
                    instructions.push(Instruction::CopyArg(top_register, skip));
                } else {
                    match return_mode {
                        ReturnMode::Static(1) => {
                            instructions.push(Instruction::CopyArg(top_register, skip));
                        }
                        ReturnMode::Extend(count) => {
                            instructions.push(Instruction::CopyVariadic(top_register, count, skip));
                        }
                        ReturnMode::UnsizedDestinationPreserve(dest) => {
                            instructions.push(Instruction::CopyUnsizedVariadic(dest, skip));
                        }
                        _ => unreachable!(),
                    }
                }

                while self.resolve_operation(top_register, result_register, operation_priority)? {}
            }
            _ => return Err(SyntaxError::new_unexpected_token(self.source, token).into()),
        }

        Ok(*result_register)
    }

    /// Updates src_register to top_register if an operation was applied
    fn resolve_operation(
        &mut self,
        top_register: Register,
        src_register: &mut Register,
        priority: u8,
    ) -> Result<bool, LuaCompilationError> {
        let Some(next_token) = self.token_iter.peek().cloned().transpose()? else {
            return Ok(false);
        };

        let (cmp_priority, next_priority) = binary_operator_priority(next_token.label);
        if cmp_priority == 0 {
            // no operation
            return Ok(false);
        }

        if cmp_priority <= priority {
            return Ok(false);
        }

        // consume token
        self.token_iter.next();

        let dest = top_register;
        let a = *src_register;
        let mut b = top_register + 1;
        // operations on function results will only use the first value
        let return_mode = ReturnMode::Static(1);

        *src_register = dest;

        match next_token.label {
            LuaTokenLabel::Plus => {
                b = self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::Add(dest, a, b));
            }
            LuaTokenLabel::Minus => {
                b = self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::Subtract(dest, a, b));
            }
            LuaTokenLabel::Star => {
                b = self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::Multiply(dest, a, b));
            }
            LuaTokenLabel::Slash => {
                b = self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::Division(dest, a, b));
            }
            LuaTokenLabel::DoubleSlash => {
                b = self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::IntegerDivision(dest, a, b));
            }
            LuaTokenLabel::Percent => {
                b = self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::Modulus(dest, a, b));
            }
            LuaTokenLabel::Caret => {
                b = self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::Power(dest, a, b));
            }
            LuaTokenLabel::Ampersand => {
                b = self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::BitwiseAnd(dest, a, b));
            }
            LuaTokenLabel::Pipe => {
                b = self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::BitwiseOr(dest, a, b));
            }
            LuaTokenLabel::Tilde => {
                b = self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::BitwiseXor(dest, a, b));
            }
            LuaTokenLabel::BitShiftLeft => {
                b = self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::BitShiftLeft(dest, a, b));
            }
            LuaTokenLabel::BitShiftRight => {
                b = self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::BitShiftRight(dest, a, b));
            }
            LuaTokenLabel::DoubleDot => {
                b = self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::Concat(dest, a, b));
            }
            LuaTokenLabel::CmpLessThan => {
                b = self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::LessThan(dest, a, b));
            }
            LuaTokenLabel::CmpLessThanEqual => {
                b = self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::LessThanEqual(dest, a, b));
            }
            LuaTokenLabel::CmpEqual => {
                b = self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::Equal(dest, a, b));
            }
            LuaTokenLabel::CmpNotEqual => {
                b = self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::Equal(dest, a, b));
                instructions.push(Instruction::Not(dest, dest));
            }
            LuaTokenLabel::CmpGreaterThan => {
                b = self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::LessThan(dest, b, a));
            }
            LuaTokenLabel::CmpGreaterThanEqual => {
                b = self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::LessThanEqual(dest, b, a));
            }
            LuaTokenLabel::And => {
                // jump / skip calculating `b`` if `a` is falsey (fail the chain)
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::TestTruthy(false, a));

                let jump_index = instructions.len();
                instructions.push(Instruction::Jump(0.into()));

                // calculate b, store directly in a's register
                b = self.resolve_expression(a, return_mode, next_priority)?;
                self.copy_stack_value(a, b);

                let instructions = &mut self.top_function.instructions;

                // resolve jump
                if instructions.len() - jump_index > 2 {
                    instructions[jump_index] = Instruction::Jump(instructions.len().into());
                } else {
                    // optimization, invert condition to remove the jump
                    instructions[jump_index - 1] = Instruction::TestTruthy(true, a);
                    instructions.remove(jump_index);
                }
            }
            LuaTokenLabel::Or => {
                // jump / skip calculating `b`` if `a` is truthy (accept the first truthy value)
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::TestTruthy(true, a));

                let jump_index = instructions.len();
                instructions.push(Instruction::Jump(0.into()));

                // calculate b in a's register as we want to swap it anyway
                b = self.resolve_expression(a, return_mode, next_priority)?;
                self.copy_stack_value(a, b);

                let instructions = &mut self.top_function.instructions;

                // resolve jump
                if instructions.len() - jump_index > 2 {
                    instructions[jump_index] = Instruction::Jump(instructions.len().into());
                } else {
                    // optimization, invert condition to remove the jump
                    instructions[jump_index - 1] = Instruction::TestTruthy(false, a);
                    instructions.remove(jump_index);
                }
            }
            _ => unreachable!(),
        }

        Ok(true)
    }

    fn resolve_table(&mut self, top_register: Register) -> Result<(), LuaCompilationError> {
        let count_register = top_register + 1;
        let list_start = top_register + 2;

        let mut initial_variadic_count = 1;

        let instructions = &mut self.top_function.instructions;
        let create_table_index = instructions.len();
        instructions.push(Instruction::CreateTable(top_register, 0));
        instructions.push(Instruction::LoadInt(count_register, 0));

        let mut next_register = list_start;
        let mut last_token;
        let mut reserve_len = 0;
        let mut variadic_instruction_range = None;

        loop {
            let token = self.expect_any()?;
            last_token = token;

            if token.label == LuaTokenLabel::CloseCurly {
                break;
            }

            // adjust last element to return a single value
            if let Some(range) = variadic_instruction_range {
                self.variadic_to_single_static(count_register, range);
                variadic_instruction_range = None;

                // see if we need to flush
                if next_register >= REGISTER_LIMIT - FLUSH_TABLE_LIMIT
                    || next_register - list_start >= FLUSH_TABLE_LIMIT
                {
                    let instructions = &mut self.top_function.instructions;
                    instructions.push(Instruction::FlushToTable(
                        top_register,
                        list_start,
                        next_register - 1,
                    ));
                    reserve_len += next_register - list_start;

                    next_register = list_start;
                }
            }

            match token.label {
                // `[exp] = exp`
                LuaTokenLabel::OpenBracket => {
                    let key_register =
                        self.resolve_expression(next_register, ReturnMode::Static(1), 0)?;

                    self.expect(LuaTokenLabel::CloseBracket)?;
                    self.expect(LuaTokenLabel::Assign)?;

                    let value_register =
                        self.resolve_expression(next_register + 1, ReturnMode::Static(1), 0)?;

                    self.top_function
                        .map_following_instructions(self.source, token.offset);

                    let instructions = &mut self.top_function.instructions;
                    instructions.push(Instruction::CopyToTableValue(
                        top_register,
                        key_register,
                        value_register,
                    ));
                }

                // `name = exp`
                LuaTokenLabel::Name
                    if self
                        .token_iter
                        .peek()
                        .cloned()
                        .transpose()?
                        .is_some_and(|peeked| peeked.label == LuaTokenLabel::Assign) =>
                {
                    // consume assignment token
                    let name_token = self.expect_any()?;

                    let string_index = self.top_function.intern_string(self.source, token)?;

                    let value_register =
                        self.resolve_expression(next_register, ReturnMode::Static(1), 0)?;

                    self.top_function
                        .map_following_instructions(self.source, name_token.offset);

                    let instructions = &mut self.top_function.instructions;
                    instructions.push(Instruction::CopyToTableField(top_register, value_register));
                    instructions.push(Instruction::Constant(string_index));
                }
                _ => {
                    let instruction_start = self.top_function.instructions.len();

                    // try reading this as an expression
                    self.resolve_partially_consumed_expression(
                        next_register,
                        ReturnMode::Extend(count_register),
                        token,
                        0,
                    )?;

                    let instruction_end = self.top_function.instructions.len();

                    variadic_instruction_range = Some(instruction_start..instruction_end);
                    next_register += 1;
                }
            }

            let token = self.expect_any()?;
            last_token = token;

            match token.label {
                LuaTokenLabel::SemiColon | LuaTokenLabel::Comma => {}
                LuaTokenLabel::CloseCurly => break,
                _ => return Err(SyntaxError::new_unexpected_token(self.source, token).into()),
            }
        }

        if next_register > list_start {
            let instructions = &mut self.top_function.instructions;

            if let Some(range) = variadic_instruction_range {
                // handling the last element as variadic

                if matches!(
                    instructions.get(range.end - 1),
                    Some(Instruction::CopyVariadic(..))
                ) {
                    // let the variadic update this
                    initial_variadic_count = 0;
                }

                if next_register - 1 > list_start {
                    // flush remaining single values
                    instructions.push(Instruction::FlushToTable(
                        top_register,
                        list_start,
                        next_register - 2,
                    ));
                }

                reserve_len += next_register - list_start - 1;

                instructions.push(Instruction::VariadicToTable(
                    top_register,
                    count_register,
                    next_register - 1,
                ));
            } else {
                // just flush the remaining single values
                instructions.push(Instruction::FlushToTable(
                    top_register,
                    list_start,
                    next_register - 1,
                ));
            }
        }

        let len_index =
            self.top_function
                .register_number(self.source, last_token, reserve_len as i64)?;

        let variadic_constant =
            self.top_function
                .register_number(self.source, last_token, initial_variadic_count)?;

        let instructions = &mut self.top_function.instructions;
        instructions[create_table_index] = Instruction::CreateTable(top_register, len_index);
        instructions[create_table_index + 1] =
            Instruction::LoadInt(count_register, variadic_constant);

        Ok(())
    }

    /// Start past LuaTokenLabel::Assign
    /// a scope should be created before this call for the name to be converted to a local
    ///
    /// Returns the loop start instruction index and the jump to end index
    fn resolve_numeric_for_params(
        &mut self,
        name_token: LuaToken<'source>,
    ) -> Result<(usize, usize), LuaCompilationError> {
        let local = self.top_function.register_local(self.source, name_token)?;

        let top_register = self.top_function.next_register;

        // resolve initial value
        let register = self.resolve_expression(top_register, ReturnMode::Static(1), 0)?;
        self.copy_stack_value(local, register);

        // resolve limit
        self.expect(LuaTokenLabel::Comma)?;
        let register = self.resolve_expression(top_register, ReturnMode::Static(1), 0)?;
        self.copy_stack_value(top_register, register);

        // resolve step
        let next_token = self.token_iter.peek().cloned().transpose()?;

        if next_token.is_some_and(|token| token.label == LuaTokenLabel::Comma) {
            // consume token
            self.token_iter.next();
            let register = self.resolve_expression(top_register + 1, ReturnMode::Static(1), 0)?;
            self.copy_stack_value(top_register + 1, register);
        } else {
            // step by 1
            let count_constant = self
                .top_function
                .register_number(self.source, name_token, 1)?;
            let instructions = &mut self.top_function.instructions;
            instructions.push(Instruction::LoadInt(top_register + 1, count_constant));
        }

        if let Some(token) = next_token {
            self.top_function
                .map_following_instructions(self.source, token.offset);
        }

        let instructions = &mut self.top_function.instructions;
        let start_index = instructions.len();
        instructions.push(Instruction::NumericFor(top_register, local));
        let jump_index = instructions.len();
        instructions.push(Instruction::Jump(0.into()));

        // consume registers for the limit and step
        self.top_function.next_register += 2;

        Ok((start_index, jump_index))
    }

    /// A scope should be created before this call for the name list to be converted to locals
    ///
    /// Returns the loop start instruction index and the jump to end index
    fn resolve_generic_for_params(
        &mut self,
        name_token: LuaToken<'source>,
        next_token: LuaToken<'source>,
    ) -> Result<(usize, usize), LuaCompilationError> {
        let mut locals = vec![self.top_function.register_local(self.source, name_token)?];
        let mut name_token;
        let mut next_token = next_token;

        loop {
            match next_token.label {
                LuaTokenLabel::Comma => {}
                LuaTokenLabel::In => break,
                _ => return Err(SyntaxError::new_unexpected_token(self.source, next_token).into()),
            }

            name_token = self.expect(LuaTokenLabel::Name)?;
            let local = self.top_function.register_local(self.source, name_token)?;
            locals.push(local);

            if next_token.label == LuaTokenLabel::In {
                break;
            }

            next_token = self.expect_any()?;
        }

        // resolve an expression and store the first result at the top register
        // swap it with a two for the function call later
        // the data after represents the invariant state and the control variable
        let top_register = self.top_function.next_register;
        self.resolve_exp_list(next_token, top_register)?;

        self.top_function
            .map_following_instructions(self.source, next_token.offset);

        let count_constant = self
            .top_function
            .register_number(self.source, next_token, 2)?;
        let instructions = &mut self.top_function.instructions;
        instructions.push(Instruction::Copy(top_register, top_register + 1));
        instructions.push(Instruction::LoadInt(top_register + 1, count_constant));

        // loop start, call the function and store the results over the control variable
        let start_index = instructions.len();
        let control_register = top_register + 3;
        instructions.push(Instruction::Call(
            top_register,
            ReturnMode::UnsizedDestinationPreserve(control_register),
        ));

        instructions.push(Instruction::TestNil(control_register));
        let jump_index = instructions.len();
        instructions.push(Instruction::Jump(0.into()));

        // assign locals
        for (i, dest) in locals.into_iter().enumerate() {
            let src = control_register + i as Register;
            self.copy_stack_value(dest, src);
        }

        // avoid overwriting control registers
        self.top_function.next_register += 4;

        Ok((start_index, jump_index))
    }
}

fn starts_function_call(label: LuaTokenLabel) -> bool {
    matches!(
        label,
        LuaTokenLabel::StringLiteral
            | LuaTokenLabel::OpenCurly
            | LuaTokenLabel::OpenParen
            | LuaTokenLabel::Colon
    )
}

fn starts_expression(label: LuaTokenLabel) -> bool {
    matches!(
        label,
        LuaTokenLabel::Nil
            | LuaTokenLabel::True
            | LuaTokenLabel::False
            | LuaTokenLabel::Numeral
            | LuaTokenLabel::StringLiteral
            | LuaTokenLabel::OpenCurly
            | LuaTokenLabel::OpenParen
            | LuaTokenLabel::Hash
            | LuaTokenLabel::Minus
            | LuaTokenLabel::Not
            | LuaTokenLabel::Tilde
            | LuaTokenLabel::Name
            | LuaTokenLabel::Function
            | LuaTokenLabel::TripleDot
    )
}

fn continues_variable_path(label: LuaTokenLabel) -> bool {
    matches!(label, LuaTokenLabel::Dot | LuaTokenLabel::OpenBracket)
}

fn unary_priority() -> u8 {
    12
}

/// https://wubingzheng.github.io/build-lua-in-rust/en/ch05-02.binary_ops.html
fn binary_operator_priority(label: LuaTokenLabel) -> (u8, u8) {
    match label {
        LuaTokenLabel::Caret => (14, 13),
        // unary operations => (12, 12),
        LuaTokenLabel::Star
        | LuaTokenLabel::Slash
        | LuaTokenLabel::DoubleSlash
        | LuaTokenLabel::Percent => (11, 11),
        LuaTokenLabel::Plus | LuaTokenLabel::Minus => (10, 10),
        LuaTokenLabel::DoubleDot => (9, 8), // concat
        LuaTokenLabel::Pipe => (7, 7),
        LuaTokenLabel::Tilde => (6, 6),
        LuaTokenLabel::Ampersand => (5, 5),
        LuaTokenLabel::BitShiftLeft | LuaTokenLabel::BitShiftRight => (4, 4), // bitwise ops
        LuaTokenLabel::CmpLessThan
        | LuaTokenLabel::CmpLessThanEqual
        | LuaTokenLabel::CmpEqual
        | LuaTokenLabel::CmpNotEqual
        | LuaTokenLabel::CmpGreaterThan
        | LuaTokenLabel::CmpGreaterThanEqual => (3, 3),
        LuaTokenLabel::And => (2, 2),
        LuaTokenLabel::Or => (1, 1),
        _ => (0, 0),
    }
}
