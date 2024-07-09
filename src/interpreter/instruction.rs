pub type Register = u8;
pub type ConstantIndex = u16;

#[derive(Clone, Copy)]
pub struct InstructionIndex([u8; 3]);

impl InstructionIndex {
    pub const MAX: usize = usize::from_le_bytes([255, 255, 255, 0, 0, 0, 0, 0]);
}

impl From<usize> for InstructionIndex {
    fn from(value: usize) -> Self {
        let [a, b, c, ..] = value.to_le_bytes();
        Self([a, b, c])
    }
}

impl From<InstructionIndex> for usize {
    fn from(index: InstructionIndex) -> usize {
        let [a, b, c] = index.0;
        usize::from_le_bytes([a, b, c, 0, 0, 0, 0, 0])
    }
}

impl std::fmt::Debug for InstructionIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", usize::from(*self))
    }
}

impl std::fmt::Display for InstructionIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", usize::from(*self))
    }
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReturnMode {
    /// Swap the function and args on the stack with an integer representing return count, followed by each return value
    #[default]
    Multi,
    /// Swap the function and args on the stack with a static amount of return values
    Static(Register),
    /// Clear the function and args on the stack, store a single result at a specific register
    Destination(Register),
    /// Swap the function and args on the stack with the return values, add the return count subtracted by one to a specific register
    Extend(Register),
    /// Stores multiple result at a specific register without a return count
    ///
    /// The destination and beyond will be cleared before placing values
    UnsizedDestinationPreserve(Register),
    /// Replace the calling function with this function, adopt the parent's ReturnMode
    TailCall,
}

#[derive(Debug, Clone, Copy)]
pub enum Instruction {
    /// Data carrying instruction that's interpreted by the previous instruction
    ///
    /// Errors when unused by the previous instruction
    Constant(ConstantIndex),

    /// Stores nil in a register
    ///
    /// (dest)
    SetNil(Register),

    /// Stores a bool in a register
    ///
    /// (dest, value)
    SetBool(Register, bool),

    /// Loads an integer from the numbers list and stores it in a register
    ///
    /// (dest, index)
    LoadInt(Register, ConstantIndex),

    /// Loads a float from the numbers list and stores it in a register
    ///
    /// (dest, index)
    LoadFloat(Register, ConstantIndex),

    /// Loads a byte string constant
    ///
    /// (dest, index)
    LoadBytes(Register, ConstantIndex),

    /// Clears values at and past the destination
    ///
    /// (dest)
    ClearFrom(Register),

    /// Loads an integer from the numbers list and stores it in a register
    ///
    /// Clears values past the destination
    ///
    /// (dest, index)
    PrepMulti(Register, ConstantIndex),

    /// Creates a table, reserves space for the list part of the table
    ///
    /// (dest, reserve_index)
    CreateTable(Register, ConstantIndex),

    /// Flushes data to a table, expects the data to follow with a one register gap after the table
    ///
    /// index_offset is the first index we use when appending,
    /// an Instruction::Constant can appear after this instruction to add to that index
    ///
    /// (dest, total, index_offset)
    FlushToTable(Register, Register, Register),

    /// Flushes data to a table
    ///
    /// src_start points to the start of the data, the count should be stored in the register after the dest
    ///
    /// index_offset is the first index we use when appending,
    /// an Instruction::Constant can appear after this instruction to add to that index
    ///
    /// (dest, src_start, index_offset)
    VariadicToTable(Register, Register, Register),

    /// Copies a value from a table onto the stack
    ///
    /// Expects the next instruction to be Instruction::Constant for the field string
    ///
    /// (dest, table)
    CopyTableField(Register, Register),

    /// Copies a value from the stack to a table
    ///
    /// Expects the next instruction to be Instruction::Constant for the field string
    ///
    /// (table, src)
    CopyToTableField(Register, Register),

    /// Copies a value from a table onto the stack
    ///
    /// (dest, table, key)
    CopyTableValue(Register, Register, Register),

    /// Copies a value from the stack to a table
    ///
    /// (table, key, src)
    CopyToTableValue(Register, Register, Register),

    /// Copies a value from an arg to the destination
    ///
    /// (dest, arg)
    CopyArg(Register, Register),

    /// Copies values from args to the destination
    ///
    /// (dest, count)
    CopyArgs(Register, Register),

    /// Copies args to the destination and increments the value at count_dest for each copied arg
    ///
    /// (dest, count_dest, skip)
    CopyVariadic(Register, Register, Register),

    /// Copies args to the destination
    ///
    /// (dest, skip)
    CopyUnsizedVariadic(Register, Register),

    /// Promotes a stack value to a heap value if it's not already
    ///
    /// Passes as an up value to the next Closure instruction
    ///
    /// (dest, src)
    Capture(Register, Register),

    /// Copies an up value to the next Closure instruction
    ///
    /// (dest, src)
    CaptureUpValue(Register, Register),

    /// Loads a function onto the stack, creates a new function if values were captured
    ///
    /// (dest, function_index)
    Closure(Register, ConstantIndex),

    /// Sets an up value to Nil
    ///
    /// (dest)
    ClearUpValue(Register),

    /// Copies an up value to the stack
    /// The first up value for a module's top level function will be initialized with the default environment
    ///
    /// (dest, src)
    CopyUpValue(Register, Register),

    /// Copies a value to an up value
    ///
    /// (dest, src)
    CopyToUpValue(Register, Register),

    /// Copies a value to a up value
    ///
    /// If the value points to another value, the pointed to value will be updated instead (used for closures)
    ///
    /// (dest, src)
    CopyToUpValueDeref(Register, Register),

    /// Copies values between stack registers
    ///
    /// (dest, src)
    Copy(Register, Register),

    /// Copies a value
    ///
    /// If the value points to another value, the pointed to value will be updated instead (used for closures)
    ///
    /// (dest, src)
    CopyToDeref(Register, Register),

    /// Copies a range of values
    ///
    /// If a value points to another value, the pointed to value will be updated instead (used for closures)
    ///
    /// Does not account for `src` < `dest`
    ///
    /// (dest, src, count)
    CopyRangeToDeref(Register, Register, Register),

    /// (dest, src)
    Len(Register, Register),

    /// (dest, src)
    Not(Register, Register),

    /// (dest, src)
    UnaryMinus(Register, Register),

    /// (dest, src)
    BitwiseNot(Register, Register),

    /// (dest, a, b)
    Add(Register, Register, Register),

    /// (dest, a, b)
    Subtract(Register, Register, Register),

    /// (dest, a, b)
    Multiply(Register, Register, Register),

    /// (dest, a, b)
    Division(Register, Register, Register),

    /// (dest, a, b)
    IntegerDivision(Register, Register, Register),

    /// (dest, a, b)
    Modulus(Register, Register, Register),

    /// (dest, a, b)
    Power(Register, Register, Register),

    /// (dest, a, b)
    BitwiseAnd(Register, Register, Register),

    /// (dest, a, b)
    BitwiseOr(Register, Register, Register),

    /// (dest, a, b)
    BitwiseXor(Register, Register, Register),

    /// (dest, a, b)
    BitShiftLeft(Register, Register, Register),

    /// (dest, a, b)
    BitShiftRight(Register, Register, Register),

    /// (dest, a, b)
    Equal(Register, Register, Register),

    /// (dest, a, b)
    LessThan(Register, Register, Register),

    /// (dest, a, b)
    LessThanEqual(Register, Register, Register),

    /// (dest, a, b)
    Concat(Register, Register, Register),

    /// Skips an instruction if (not not src) ~= expected
    ///
    /// (expected, src)
    TestTruthy(bool, Register),

    /// Skips an instruction if the value is not nil
    ///
    /// (src)
    TestNil(Register),

    /// Expects two numbers at src, the limit and the step
    ///
    /// Jump using JumpToForLoop to increment the local
    ///
    /// (src, local)
    NumericFor(Register, Register),

    JumpToForLoop(InstructionIndex),

    Jump(InstructionIndex),

    /// Expects: function, arg count, ...args, at the specified register
    ///
    /// (register, return_mode)
    Call(Register, ReturnMode),

    /// Expects: return count, ...value, at the speicified register
    ///
    /// (register)
    Return(Register),
}

impl Instruction {
    pub fn name(&self) -> &'static str {
        match self {
            Instruction::Constant(_) => "Constant",
            Instruction::SetNil(_) => "SetNil",
            Instruction::SetBool(_, _) => "SetBool",
            Instruction::LoadInt(_, _) => "LoadInt",
            Instruction::LoadFloat(_, _) => "LoadFloat",
            Instruction::LoadBytes(_, _) => "LoadBytes",
            Instruction::ClearFrom(_) => "ClearFrom",
            Instruction::PrepMulti(_, _) => "PrepMulti",
            Instruction::CreateTable(_, _) => "CreateTable",
            Instruction::FlushToTable(_, _, _) => "FlushToTable",
            Instruction::VariadicToTable(_, _, _) => "VariadicToTable",
            Instruction::CopyTableField(_, _) => "CopyTableField",
            Instruction::CopyToTableField(_, _) => "CopyToTableField",
            Instruction::CopyTableValue(_, _, _) => "CopyTableValue",
            Instruction::CopyToTableValue(_, _, _) => "CopyToTableValue",
            Instruction::CopyArg(_, _) => "CopyArg",
            Instruction::CopyArgs(_, _) => "CopyArgs",
            Instruction::CopyVariadic(_, _, _) => "CopyVariadic",
            Instruction::CopyUnsizedVariadic(_, _) => "CopyUnsizedVariadic",
            Instruction::Capture(_, _) => "Capture",
            Instruction::CaptureUpValue(_, _) => "CaptureUpValue",
            Instruction::Closure(_, _) => "Closure",
            Instruction::ClearUpValue(_) => "ClearUpValue",
            Instruction::CopyUpValue(_, _) => "CopyUpValue",
            Instruction::CopyToUpValue(_, _) => "CopyToUpValue",
            Instruction::CopyToUpValueDeref(_, _) => "CopyToUpValueDeref",
            Instruction::Copy(_, _) => "Copy",
            Instruction::CopyToDeref(_, _) => "CopyToDeref",
            Instruction::CopyRangeToDeref(_, _, _) => "CopyRangeToDeref",
            Instruction::Len(_, _) => "Len",
            Instruction::Not(_, _) => "Not",
            Instruction::UnaryMinus(_, _) => "UnaryMinus",
            Instruction::BitwiseNot(_, _) => "BitwiseNot",
            Instruction::Add(_, _, _) => "Add",
            Instruction::Subtract(_, _, _) => "Subtract",
            Instruction::Multiply(_, _, _) => "Multiply",
            Instruction::Division(_, _, _) => "Division",
            Instruction::IntegerDivision(_, _, _) => "IntegerDivision",
            Instruction::Modulus(_, _, _) => "Modulus",
            Instruction::Power(_, _, _) => "Power",
            Instruction::BitwiseAnd(_, _, _) => "BitwiseAnd",
            Instruction::BitwiseOr(_, _, _) => "BitwiseOr",
            Instruction::BitwiseXor(_, _, _) => "BitwiseXor",
            Instruction::BitShiftLeft(_, _, _) => "BitShiftLeft",
            Instruction::BitShiftRight(_, _, _) => "BitShiftRight",
            Instruction::Equal(_, _, _) => "Equal",
            Instruction::LessThan(_, _, _) => "LessThan",
            Instruction::LessThanEqual(_, _, _) => "LessThanEqual",
            Instruction::Concat(_, _, _) => "Concat",
            Instruction::TestTruthy(_, _) => "TestTruthy",
            Instruction::TestNil(_) => "TestNil",
            Instruction::NumericFor(_, _) => "NumericFor",
            Instruction::JumpToForLoop(_) => "JumpToForLoop",
            Instruction::Jump(_) => "Jump",
            Instruction::Call(_, _) => "Call",
            Instruction::Return(_) => "Return",
        }
    }
}

#[cfg(feature = "instruction_exec_counts")]
#[derive(Default)]
pub(crate) struct InstructionCounter {
    map: crate::FastHashMap<std::mem::Discriminant<Instruction>, (Instruction, usize)>,
}

#[cfg(feature = "instruction_exec_counts")]
impl InstructionCounter {
    pub(crate) fn clear(&mut self) {
        self.map.clear();
    }

    pub(crate) fn track(&mut self, instruction: Instruction) {
        self.map
            .entry(std::mem::discriminant(&instruction))
            .and_modify(|(_, count)| *count += 1)
            .or_insert((instruction, 1));
    }

    pub(crate) fn data(&self) -> impl Iterator<Item = (&'static str, usize)> + '_ {
        self.map
            .values()
            .map(|(instruction, count)| (instruction.name(), *count))
    }
}
