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
    /// Local register
    Local(Register),
    /// Two stack registers: table and key
    TableValue(LuaToken<'source>, Register, Register),
    /// Two stack registers: table and key
    TableField(LuaToken<'source>, Register, ConstantIndex),
    /// stack register
    Collapsed(Register),
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
    first_local: Register,
    locals: FastHashMap<&'source str, Register>,
}

struct Capture<'source> {
    token: LuaToken<'source>,
    parent: usize,
    parent_local: Register,
    local: Register,
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
    next_local: Register,
    next_capture_local: Register,
}

impl<'source> FunctionContext<'source> {
    fn push_scope(&mut self) {
        let old_scope = std::mem::take(&mut self.top_scope);
        self.top_scope.first_local = self.next_local;
        self.scopes.push(old_scope);
    }

    fn pop_scope(&mut self) {
        // recycle locals
        self.next_local = self.top_scope.first_local;
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

    fn register_capture(
        &mut self,
        source: &'source str,
        token: LuaToken<'source>,
        parent: usize,
        parent_local: Register,
    ) -> Result<Register, LuaCompilationError> {
        let index = self.next_capture_local;

        if index > MAX_LOCALS {
            return Err(LuaCompilationError::new_too_many_locals(
                source,
                token.offset,
            ));
        }

        self.next_capture_local += 1;

        self.captures.push(Capture {
            token,
            parent,
            parent_local,
            local: index,
        });

        // store on the root scope as well
        if let Some(scope) = self.scopes.first_mut() {
            scope.locals.insert(token.content, index);
        }

        self.top_scope.locals.insert(token.content, index);
        Ok(index)
    }

    fn register_local(
        &mut self,
        source: &'source str,
        token: LuaToken<'source>,
    ) -> Result<Register, LuaCompilationError> {
        // make sure we don't overlap with a capture
        for capture in &self.captures {
            if capture.local == self.next_local {
                self.next_local += 1;
            }
        }

        if self.next_local > MAX_LOCALS {
            return Err(LuaCompilationError::new_too_many_locals(
                source,
                token.offset,
            ));
        }

        if self.next_local == self.next_capture_local {
            self.next_capture_local += 1;
        }

        let index = self.next_local;
        self.top_scope.locals.insert(token.content, index);
        self.next_local += 1;
        Ok(index)
    }

    fn local_index(&self, name: &'source str) -> Option<Register> {
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
        self.top_function.accept_variadic = true;
        self.top_function.top_scope.locals.insert(ENV_NAME, 0);
        self.top_function.next_local = 1;
        self.top_function.next_capture_local = 1;

        self.resolve_block(0)?;

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
                    let local_index = self.top_function.register_local(self.source, token)?;
                    let instructions = &mut self.top_function.instructions;
                    instructions.push(Instruction::CopyArgToLocal(local_index, named_count));

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

    fn resolve_block(&mut self, top_register: Register) -> Result<(), LuaCompilationError> {
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
                    self.resolve_block(top_register)?;
                    self.top_function.pop_scope();

                    self.expect(LuaTokenLabel::End)?;
                }
                LuaTokenLabel::If => {
                    // consume token
                    self.token_iter.next();

                    // todo: recycle?
                    let mut end_jumps = Vec::new();

                    loop {
                        self.resolve_expression(top_register, ReturnMode::Static(1), 0)?;
                        self.expect(LuaTokenLabel::Then)?;

                        let instructions = &mut self.top_function.instructions;
                        instructions.push(Instruction::TestTruthy(false, top_register));
                        let branch_index = instructions.len();
                        instructions.push(Instruction::Jump(0.into()));

                        self.top_function.push_scope();
                        self.resolve_block(top_register)?;
                        self.top_function.pop_scope();

                        // insert + track end jump
                        let instructions = &mut self.top_function.instructions;
                        end_jumps.push(instructions.len());
                        instructions.push(Instruction::Jump(0.into()));

                        // update branch jump
                        instructions[branch_index] = Instruction::Jump(instructions.len().into());

                        let end_token = self.expect_any()?;

                        match end_token.label {
                            LuaTokenLabel::ElseIf => {}
                            LuaTokenLabel::Else => {
                                self.top_function.push_scope();
                                self.resolve_block(top_register)?;
                                self.top_function.pop_scope();
                                self.expect(LuaTokenLabel::End)?;
                                break;
                            }
                            LuaTokenLabel::End => break,
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

                    self.resolve_expression(top_register, ReturnMode::Static(1), 0)?;
                    self.expect(LuaTokenLabel::Do)?;

                    let instructions = &mut self.top_function.instructions;
                    instructions.push(Instruction::TestTruthy(false, top_register));
                    let branch_index = instructions.len();
                    instructions.push(Instruction::Jump(0.into()));

                    self.top_function.push_scope();
                    self.resolve_block(top_register)?;
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
                    self.resolve_block(top_register)?;
                    self.expect(LuaTokenLabel::Until)?;
                    // we'll pop scope later

                    // test to see if we need to jump back to the start
                    self.resolve_expression(top_register, ReturnMode::Static(1), 0)?;
                    let instructions = &mut self.top_function.instructions;
                    instructions.push(Instruction::TestTruthy(false, top_register));
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

                    let mut block_register = top_register;
                    let start_index;
                    let branch_index;

                    match next_token.label {
                        LuaTokenLabel::Assign => {
                            (start_index, branch_index) =
                                self.resolve_numeric_for_params(top_register, name_token)?;

                            block_register += 2;
                        }
                        LuaTokenLabel::Comma | LuaTokenLabel::In => {
                            (start_index, branch_index) = self.resolve_generic_for_params(
                                top_register,
                                name_token,
                                next_token,
                            )?;

                            // avoid overwriting values needed for the next call
                            block_register += 4;
                        }
                        _ => {
                            return Err(
                                SyntaxError::new_unexpected_token(self.source, next_token).into()
                            )
                        }
                    }

                    self.test_register_limit(token, block_register)?;

                    self.expect(LuaTokenLabel::Do)?;
                    self.resolve_block(block_register)?;
                    self.top_function.pop_scope();
                    self.expect(LuaTokenLabel::End)?;

                    let instructions = &mut self.top_function.instructions;

                    // jump back to the start
                    instructions.push(Instruction::Jump(start_index.into()));

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
                        VariablePath::Local(local) => {
                            self.resolve_function_body(top_register, is_method)?;
                            let instructions = &mut self.top_function.instructions;
                            instructions.push(Instruction::CopyToLocalDeref(local, top_register));
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
                            instructions.push(Instruction::CopyToTableField(
                                table_index,
                                string_index,
                                top_register + 1,
                            ));
                        }
                        VariablePath::Collapsed(_) => unreachable!(),
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
                        let local_index =
                            self.top_function.register_local(self.source, name_token)?;

                        // store nil since the function may try to refer to itself and we don't want it promoting + using an old value
                        let instructions = &mut self.top_function.instructions;
                        instructions.push(Instruction::ClearLocal(local_index));

                        self.resolve_function_body(top_register, false)?;

                        let instructions = &mut self.top_function.instructions;
                        instructions.push(Instruction::CopyToLocalDeref(local_index, top_register));
                    } else {
                        let name_token = self.expect(LuaTokenLabel::Name)?;
                        let first_lhs_index =
                            self.top_function.register_local(self.source, name_token)?;

                        // todo: maybe recycle this?
                        let mut lhs_list = Vec::new();

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
                            let local_index =
                                self.top_function.register_local(self.source, name_token)?;
                            lhs_list.push(local_index);
                        }

                        if peeked_token.is_some_and(|token| token.label == LuaTokenLabel::Assign) {
                            // consume token
                            self.token_iter.next();

                            let mut next_register = top_register;

                            self.resolve_exp_list(token, next_register)?;

                            // increment to skip the len
                            next_register += 1;
                            let instructions = &mut self.top_function.instructions;

                            instructions
                                .push(Instruction::CopyToLocal(first_lhs_index, next_register));

                            let instructions = &mut self.top_function.instructions;

                            for index in lhs_list {
                                next_register += 1;
                                instructions.push(Instruction::CopyToLocal(index, next_register));
                            }
                        } else {
                            let instructions = &mut self.top_function.instructions;
                            instructions.push(Instruction::ClearLocal(first_lhs_index));

                            for index in lhs_list {
                                instructions.push(Instruction::ClearLocal(index));
                            }
                        }
                    }
                }
                LuaTokenLabel::OpenParen | LuaTokenLabel::Name => {
                    // consume token
                    self.token_iter.next();

                    let path =
                        self.resolve_variable_path(top_register, token, ReturnMode::Static(0))?;

                    if matches!(path, VariablePath::Collapsed(_)) {
                        continue;
                    }

                    let next_token = self.expect_any()?;

                    if next_token.label == LuaTokenLabel::Comma {
                        // todo: maybe recycle this?
                        let mut lhs_list = vec![path];

                        let mut next_register = top_register;

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

                            if matches!(path, VariablePath::Collapsed(_)) {
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

                            let instruction = match path {
                                VariablePath::Local(local) => {
                                    Instruction::CopyToLocalDeref(local, next_register)
                                }
                                VariablePath::TableValue(token, table_index, key_index) => {
                                    self.top_function
                                        .map_following_instructions(self.source, token.offset);

                                    Instruction::CopyToTableValue(
                                        table_index,
                                        key_index,
                                        next_register,
                                    )
                                }
                                VariablePath::TableField(token, table_index, string_index) => {
                                    self.top_function
                                        .map_following_instructions(self.source, token.offset);

                                    Instruction::CopyToTableField(
                                        table_index,
                                        string_index,
                                        next_register,
                                    )
                                }
                                VariablePath::Collapsed(_) => unreachable!(),
                            };

                            let instructions = &mut self.top_function.instructions;
                            instructions.push(instruction);
                        }
                    } else if next_token.label == LuaTokenLabel::Assign {
                        match path {
                            VariablePath::Local(local) => {
                                self.resolve_expression(
                                    top_register + 1,
                                    ReturnMode::Static(1),
                                    0,
                                )?;

                                let instructions = &mut self.top_function.instructions;
                                instructions
                                    .push(Instruction::CopyToLocalDeref(local, top_register + 1));
                            }
                            VariablePath::TableValue(_, table_index, key_index) => {
                                self.resolve_expression(
                                    top_register + 2,
                                    ReturnMode::Static(1),
                                    0,
                                )?;

                                let instructions = &mut self.top_function.instructions;
                                instructions.push(Instruction::CopyToTableValue(
                                    table_index,
                                    key_index,
                                    top_register + 2,
                                ));
                            }
                            VariablePath::TableField(token, table_index, string_index) => {
                                self.resolve_expression(
                                    top_register + 1,
                                    ReturnMode::Static(1),
                                    0,
                                )?;

                                self.top_function
                                    .map_following_instructions(self.source, token.offset);

                                let instructions = &mut self.top_function.instructions;
                                instructions.push(Instruction::CopyToTableField(
                                    table_index,
                                    string_index,
                                    top_register + 1,
                                ));
                            }
                            VariablePath::Collapsed(_) => unreachable!(),
                        }

                        if let Some(token) = self.token_iter.peek().cloned().transpose()? {
                            if token.label == LuaTokenLabel::Comma {
                                // consume token
                                self.token_iter.next();
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
    /// if the final path is VariablePath::Collapsed, assume it's a function call
    fn resolve_variable_path(
        &mut self,
        top_register: Register,
        token: LuaToken<'source>,
        return_mode_hint: ReturnMode,
    ) -> Result<VariablePath<'source>, LuaCompilationError> {
        let mut path = match token.label {
            LuaTokenLabel::Name => self.resolve_name_path(top_register, token)?,
            LuaTokenLabel::OpenParen => {
                self.resolve_expression(top_register, ReturnMode::Static(1), 0)?;
                self.expect(LuaTokenLabel::CloseParen)?;

                VariablePath::Collapsed(top_register)
            }
            _ => return Err(SyntaxError::new_unexpected_token(self.source, token).into()),
        };

        loop {
            let Some(token) = self.token_iter.peek().cloned().transpose()? else {
                break;
            };

            match token.label {
                LuaTokenLabel::Dot => {
                    self.copy_variable(top_register, path);

                    // consume dot
                    self.token_iter.next();

                    let token = self.expect(LuaTokenLabel::Name)?;

                    let string_index = self.top_function.intern_string(self.source, token)?;
                    path = VariablePath::TableField(token, top_register, string_index);
                }
                LuaTokenLabel::OpenBracket => {
                    self.copy_variable(top_register, path);

                    // consume bracket
                    self.token_iter.next();

                    self.resolve_expression(top_register + 1, ReturnMode::Static(1), 0)?;

                    self.expect(LuaTokenLabel::CloseBracket)?;

                    let key_index = top_register + 1;
                    path = VariablePath::TableValue(token, top_register, key_index);
                }
                _ if starts_function_call(token.label) => {
                    self.copy_variable(top_register, path);

                    // consume token
                    self.token_iter.next();

                    self.resolve_function_call(token, top_register, return_mode_hint)?;

                    path = VariablePath::Collapsed(top_register);
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
    /// never returns VariablePath::Collapsed
    fn resolve_name_path(
        &mut self,
        top_register: Register,
        token: LuaToken<'source>,
    ) -> Result<VariablePath<'source>, LuaCompilationError> {
        let path = if let Some(index) = self.top_function.local_index(token.content) {
            VariablePath::Local(index)
        } else if let Some(index) = self.try_capture(token)? {
            // capture
            VariablePath::Local(index)
        } else {
            // treat as environment variable
            let env_index = if let Some(index) = self.top_function.local_index(ENV_NAME) {
                index
            } else {
                self.try_capture(LuaToken {
                    label: LuaTokenLabel::Name,
                    content: ENV_NAME,
                    offset: token.offset,
                })?
                .unwrap()
            };

            let string_index = self.top_function.intern_string(self.source, token)?;

            let instructions = &mut self.top_function.instructions;
            instructions.push(Instruction::CopyLocal(top_register, env_index));

            VariablePath::TableField(token, top_register, string_index)
        };

        Ok(path)
    }

    fn try_capture(
        &mut self,
        token: LuaToken<'source>,
    ) -> Result<Option<Register>, LuaCompilationError> {
        for (i, function) in self.function_stack.iter().rev().enumerate() {
            if let Some(index) = function.top_scope.locals.get(token.content) {
                return Ok(Some(self.top_function.register_capture(
                    self.source,
                    token,
                    i,
                    *index,
                )?));
            }

            for scope in &function.scopes {
                if let Some(index) = scope.locals.get(token.content) {
                    return Ok(Some(self.top_function.register_capture(
                        self.source,
                        token,
                        i,
                        *index,
                    )?));
                }
            }
        }

        Ok(None)
    }

    fn copy_variable(&mut self, dest_index: Register, path: VariablePath) {
        match path {
            VariablePath::Local(local) => {
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::CopyLocal(dest_index, local));
            }
            VariablePath::TableValue(token, table_index, key_index) => {
                self.top_function
                    .map_following_instructions(self.source, token.offset);

                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::CopyTableValue(
                    dest_index,
                    table_index,
                    key_index,
                ));
            }
            VariablePath::TableField(token, table_index, string_index) => {
                self.top_function
                    .map_following_instructions(self.source, token.offset);

                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::CopyTableField(
                    dest_index,
                    table_index,
                    string_index,
                ));
            }
            VariablePath::Collapsed(register) => {
                if dest_index != register {
                    let instructions = &mut self.top_function.instructions;
                    instructions.push(Instruction::Copy(dest_index, register));
                }
            }
        };
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
            let local_index = self
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
            instructions.push(Instruction::CopyArgToLocal(local_index, 0));

            implicit_param_count = 1;
        }

        self.resolve_parameters(implicit_param_count)?;
        self.resolve_block(0)?;
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
        for capture in function.captures {
            let local_index = if capture.parent == 0 {
                capture.parent_local
            } else {
                self.top_function.register_capture(
                    self.source,
                    capture.token,
                    capture.parent - 1,
                    capture.parent_local,
                )?
            };

            let instructions = &mut self.top_function.instructions;
            instructions.push(Instruction::Capture(capture.local, local_index));
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
                instructions.push(Instruction::CopyTableField(
                    top_register,
                    top_register + 2,
                    string_index,
                ));
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
        instructions.push(Instruction::LoadInt(top_register, count_constant));

        let Some(next_token) = self.token_iter.peek().cloned().transpose()? else {
            return Ok(());
        };

        if !starts_expression(next_token.label) {
            return Ok(());
        }

        // clear anything still on the stack past the length
        instructions.push(Instruction::Clear(top_register + 1));

        // track the start of the first and last expression for updating function return modes to variadic
        let first_expression_start = instructions.len();
        let mut last_expression_start = instructions.len();

        // resolve the first expression
        self.resolve_expression(top_register + 1, ReturnMode::Extend(top_register), 0)?;
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

            self.resolve_expression(
                top_register + 1 + total,
                ReturnMode::Extend(top_register),
                0,
            )?;
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
        instructions[count_instruction_index] = Instruction::LoadInt(top_register, count_constant);

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
                    // need to test the target in case of nested exp_list
                    if *mode == ReturnMode::Extend(count_register) {
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

    fn resolve_expression(
        &mut self,
        top_register: Register,
        return_mode: ReturnMode,
        operation_priority: u8,
    ) -> Result<(), LuaCompilationError> {
        let token = self.expect_any()?;
        self.resolve_partially_consumed_expression(
            top_register,
            return_mode,
            token,
            operation_priority,
        )
    }

    fn resolve_partially_consumed_expression(
        &mut self,
        top_register: Register,
        return_mode: ReturnMode,
        token: LuaToken<'source>,
        operation_priority: u8,
    ) -> Result<(), LuaCompilationError> {
        self.test_register_limit(token, top_register)?;

        match token.label {
            LuaTokenLabel::Nil => {
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::SetNil(top_register));
                while self.resolve_operation(top_register, operation_priority)? {}
            }
            LuaTokenLabel::True => {
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::SetBool(top_register, true));
                while self.resolve_operation(top_register, operation_priority)? {}
            }
            LuaTokenLabel::False => {
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::SetBool(top_register, false));
                while self.resolve_operation(top_register, operation_priority)? {}
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
                while self.resolve_operation(top_register, operation_priority)? {}
            }
            LuaTokenLabel::StringLiteral => {
                let string_index = self.top_function.intern_string(self.source, token)?;
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::LoadBytes(top_register, string_index));
                while self.resolve_operation(top_register, operation_priority)? {}
            }
            LuaTokenLabel::OpenCurly => {
                self.resolve_table(top_register)?;
                while self.resolve_operation(top_register, operation_priority)? {}
            }
            LuaTokenLabel::Hash => {
                // len
                self.resolve_expression(top_register, return_mode, unary_priority())?;

                self.top_function
                    .map_following_instructions(self.source, token.offset);

                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::Len(top_register, top_register));

                while self.resolve_operation(top_register, operation_priority)? {}
            }
            LuaTokenLabel::Minus => {
                // unary minus
                self.resolve_expression(top_register, return_mode, unary_priority())?;
                while self.resolve_operation(top_register, unary_priority())? {}
                while self.resolve_operation(top_register, operation_priority)? {}

                self.top_function
                    .map_following_instructions(self.source, token.offset);

                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::UnaryMinus(top_register, top_register));
            }
            LuaTokenLabel::Not => {
                // not
                self.resolve_expression(top_register, return_mode, unary_priority())?;
                while self.resolve_operation(top_register, unary_priority())? {}
                while self.resolve_operation(top_register, operation_priority)? {}

                self.top_function
                    .map_following_instructions(self.source, token.offset);

                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::Not(top_register, top_register));
            }
            LuaTokenLabel::Tilde => {
                // bitwise not
                self.resolve_expression(top_register, return_mode, unary_priority())?;
                while self.resolve_operation(top_register, unary_priority())? {}
                while self.resolve_operation(top_register, operation_priority)? {}

                self.top_function
                    .map_following_instructions(self.source, token.offset);

                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::BitwiseNot(top_register, top_register));
            }
            LuaTokenLabel::OpenParen | LuaTokenLabel::Name => {
                // variable or function
                let path = self.resolve_variable_path(top_register, token, return_mode)?;
                self.copy_variable(top_register, path);

                while self.resolve_operation(top_register, operation_priority)? {}
            }
            LuaTokenLabel::Function => {
                self.resolve_function_body(top_register, false)?;
                while self.resolve_operation(top_register, operation_priority)? {}
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
                        _ => unreachable!(),
                    }
                }

                while self.resolve_operation(top_register, operation_priority)? {}
            }
            _ => {
                return Err(SyntaxError::new_unexpected_token(self.source, token).into());
            }
        }

        Ok(())
    }

    fn resolve_operation(
        &mut self,
        top_register: Register,
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

        let a = top_register;
        let b = top_register + 1;
        // operations on function results will only use the first value
        let return_mode = ReturnMode::Static(1);

        match next_token.label {
            LuaTokenLabel::Plus => {
                self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::Add(a, a, b));
            }
            LuaTokenLabel::Minus => {
                self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::Subtract(a, a, b));
            }
            LuaTokenLabel::Star => {
                self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::Multiply(a, a, b));
            }
            LuaTokenLabel::Slash => {
                self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::Division(a, a, b));
            }
            LuaTokenLabel::DoubleSlash => {
                self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::IntegerDivision(a, a, b));
            }
            LuaTokenLabel::Percent => {
                self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::Modulus(a, a, b));
            }
            LuaTokenLabel::Caret => {
                self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::Power(a, a, b));
            }
            LuaTokenLabel::Ampersand => {
                self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::BitwiseAnd(a, a, b));
            }
            LuaTokenLabel::Pipe => {
                self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::BitwiseOr(a, a, b));
            }
            LuaTokenLabel::Tilde => {
                self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::BitwiseXor(a, a, b));
            }
            LuaTokenLabel::BitShiftLeft => {
                self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::BitShiftLeft(a, a, b));
            }
            LuaTokenLabel::BitShiftRight => {
                self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::BitShiftRight(a, a, b));
            }
            LuaTokenLabel::DoubleDot => {
                self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::Concat(a, a, b));
            }
            LuaTokenLabel::CmpLessThan => {
                self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::LessThan(a, a, b));
            }
            LuaTokenLabel::CmpLessThanEqual => {
                self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::LessThanEqual(a, a, b));
            }
            LuaTokenLabel::CmpEqual => {
                self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::Equal(a, a, b));
            }
            LuaTokenLabel::CmpNotEqual => {
                self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::Equal(a, a, b));
                instructions.push(Instruction::Not(a, a));
            }
            LuaTokenLabel::CmpGreaterThan => {
                self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::LessThan(a, b, a));
            }
            LuaTokenLabel::CmpGreaterThanEqual => {
                self.resolve_expression(b, return_mode, next_priority)?;
                self.top_function
                    .map_following_instructions(self.source, next_token.offset);
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::LessThanEqual(a, b, a));
            }
            LuaTokenLabel::And => {
                // jump / skip calculating `b`` if `a` is falsey (fail the chain)
                let instructions = &mut self.top_function.instructions;
                instructions.push(Instruction::TestTruthy(false, a));

                let jump_index = instructions.len();
                instructions.push(Instruction::Jump(0.into()));

                // calculate b, store directly in a
                self.resolve_expression(a, return_mode, next_priority)?;

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

                // calculate b in a as we want to swap it anyway
                self.resolve_expression(a, return_mode, next_priority)?;

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
                    self.resolve_expression(next_register, ReturnMode::Static(1), 0)?;

                    self.expect(LuaTokenLabel::CloseBracket)?;
                    self.expect(LuaTokenLabel::Assign)?;

                    self.resolve_expression(next_register + 1, ReturnMode::Static(1), 0)?;

                    let instructions = &mut self.top_function.instructions;
                    instructions.push(Instruction::CopyToTableValue(
                        top_register,
                        next_register,
                        next_register + 1,
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
                    self.token_iter.next();

                    let string_index = self.top_function.intern_string(self.source, token)?;

                    self.resolve_expression(next_register, ReturnMode::Static(1), 0)?;

                    let instructions = &mut self.top_function.instructions;
                    instructions.push(Instruction::CopyToTableField(
                        top_register,
                        string_index,
                        next_register,
                    ));
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
        top_register: Register,
        name_token: LuaToken<'source>,
    ) -> Result<(usize, usize), LuaCompilationError> {
        let local_index = self.top_function.register_local(self.source, name_token)?;

        // resolve initial value
        self.resolve_expression(top_register, ReturnMode::Static(1), 0)?;

        let instructions = &mut self.top_function.instructions;
        instructions.push(Instruction::CopyToLocal(local_index, top_register));

        // resolve limit
        self.expect(LuaTokenLabel::Comma)?;
        self.resolve_expression(top_register, ReturnMode::Static(1), 0)?;

        // resolve step
        let next_token = self.token_iter.peek().cloned().transpose()?;

        if next_token.is_some_and(|token| token.label == LuaTokenLabel::Comma) {
            // consume token
            self.token_iter.next();
            self.resolve_expression(top_register + 1, ReturnMode::Static(1), 0)?;
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
        instructions.push(Instruction::PrepNumericFor(top_register, local_index));

        let start_index = instructions.len();
        instructions.push(Instruction::TestNumericFor(top_register, local_index));
        let jump_index = instructions.len();
        instructions.push(Instruction::Jump(0.into()));

        Ok((start_index, jump_index))
    }

    /// A scope should be created before this call for the name list to be converted to locals
    ///
    /// Returns the loop start instruction index and the jump to end index
    fn resolve_generic_for_params(
        &mut self,
        top_register: Register,
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
        for (i, local) in locals.into_iter().enumerate() {
            instructions.push(Instruction::CopyToLocal(
                local,
                control_register + i as Register,
            ));
        }

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
