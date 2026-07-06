pub type Register = u8;
pub type ConstantIndex = u16;

#[derive(Clone, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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
    /// Stores multiple results at a specific register without a return count
    ///
    /// The destination and beyond will be cleared before placing values
    UnsizedDestinationPreserve(Register),
    /// Coerces the result to a boolean. Used by comparison operators
    Boolean(Register),
    /// Replace the calling function with this function, adopt the parent's ReturnMode
    TailCall,
    /// Used internally to re-enable hooks after a hook finishes
    Hook,
}

#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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

    /// Stores an integer in a register
    ///
    /// (dest, value)
    SetInt(Register, i16),

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

    /// Stores an integer at the destination and clears values past it
    ///
    /// (dest, value)
    PrepMulti(Register, i16),

    /// Moves a table at the dest two registers to the right, copies a field from that table into the dest
    ///
    /// (dest, bytes_index)
    PrepSelf(Register, ConstantIndex),

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

    /// Loads a function onto the stack, creates a new function if values were captured
    ///
    /// (dest, function_index)
    Closure(Register, ConstantIndex),

    /// Copies an up value to the stack
    /// The first up value for a module's top level function will be initialized with the default environment
    ///
    /// (dest, src)
    CopyUpValue(Register, Register),

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
    /// (dest, src, count)
    CopyRange(Register, Register, Register),

    /// Copies a range of values
    ///
    /// If a value points to another value, the pointed to value will be updated instead (used for closures)
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
    /// (src, expected)
    TestTruthy(Register, bool),

    /// Skips an instruction and sets dest to src if (not not src) ~= expected
    ///
    /// (dest, src, expected)
    TestTruthyThenCopy(Register, Register, bool),

    /// Skips an instruction if the value is not nil
    ///
    /// (src)
    TestNil(Register),

    /// Expects three numbers at src: a value to increment, a limit, and the step
    ///
    /// Increments the instruction counter by forward_jump when complete
    ///
    /// Jump using JumpToForLoop to increment the local
    ///
    /// (src, forward_jump)
    NumericFor(Register, u16),

    /// Expects an iterator function, invariant state, and control variable
    ///
    /// Calls the iterator function, storing an unsized result to the right of the control variable
    ///
    /// (register)
    GenericFor(u8),

    /// Expects an iterator function, invariant state, control variable, and result from the GenericFor call
    ///
    /// Tests the result. If it isn't nil, the result will be copied into the control variable and the next instruction will be skipped
    ///
    /// (register)
    GenericForTest(u8),

    JumpToForLoop(InstructionIndex),

    Jump(InstructionIndex),

    /// Expects: function, arg count, ...args, at the specified register
    ///
    /// (register, return_mode)
    Call(Register, ReturnMode),

    /// Expects: return count, ...value, at the specified register
    ///
    /// (register)
    Return(Register),
}

impl Instruction {
    pub fn name(&self) -> &'static str {
        match self {
            Instruction::Constant(..) => "Constant",
            Instruction::SetNil(..) => "SetNil",
            Instruction::SetBool(..) => "SetBool",
            Instruction::SetInt(..) => "SetInt",
            Instruction::LoadInt(..) => "LoadInt",
            Instruction::LoadFloat(..) => "LoadFloat",
            Instruction::LoadBytes(..) => "LoadBytes",
            Instruction::ClearFrom(..) => "ClearFrom",
            Instruction::PrepMulti(..) => "PrepMulti",
            Instruction::PrepSelf(..) => "PrepSelf",
            Instruction::CreateTable(..) => "CreateTable",
            Instruction::FlushToTable(..) => "FlushToTable",
            Instruction::VariadicToTable(..) => "VariadicToTable",
            Instruction::CopyTableField(..) => "CopyTableField",
            Instruction::CopyToTableField(..) => "CopyToTableField",
            Instruction::CopyTableValue(..) => "CopyTableValue",
            Instruction::CopyToTableValue(..) => "CopyToTableValue",
            Instruction::CopyArg(..) => "CopyArg",
            Instruction::CopyArgs(..) => "CopyArgs",
            Instruction::CopyVariadic(..) => "CopyVariadic",
            Instruction::CopyUnsizedVariadic(..) => "CopyUnsizedVariadic",
            Instruction::Closure(..) => "Closure",
            Instruction::CopyUpValue(..) => "CopyUpValue",
            Instruction::CopyToUpValueDeref(..) => "CopyToUpValueDeref",
            Instruction::Copy(..) => "Copy",
            Instruction::CopyToDeref(..) => "CopyToDeref",
            Instruction::CopyRange(..) => "CopyRange",
            Instruction::CopyRangeToDeref(..) => "CopyRangeToDeref",
            Instruction::Len(..) => "Len",
            Instruction::Not(..) => "Not",
            Instruction::UnaryMinus(..) => "UnaryMinus",
            Instruction::BitwiseNot(..) => "BitwiseNot",
            Instruction::Add(..) => "Add",
            Instruction::Subtract(..) => "Subtract",
            Instruction::Multiply(..) => "Multiply",
            Instruction::Division(..) => "Division",
            Instruction::IntegerDivision(..) => "IntegerDivision",
            Instruction::Modulus(..) => "Modulus",
            Instruction::Power(..) => "Power",
            Instruction::BitwiseAnd(..) => "BitwiseAnd",
            Instruction::BitwiseOr(..) => "BitwiseOr",
            Instruction::BitwiseXor(..) => "BitwiseXor",
            Instruction::BitShiftLeft(..) => "BitShiftLeft",
            Instruction::BitShiftRight(..) => "BitShiftRight",
            Instruction::Equal(..) => "Equal",
            Instruction::LessThan(..) => "LessThan",
            Instruction::LessThanEqual(..) => "LessThanEqual",
            Instruction::Concat(..) => "Concat",
            Instruction::TestTruthy(..) => "TestTruthy",
            Instruction::TestTruthyThenCopy(..) => "TestTruthyThenCopy",
            Instruction::TestNil(..) => "TestNil",
            Instruction::NumericFor(..) => "NumericFor",
            Instruction::GenericFor(..) => "GenericFor",
            Instruction::GenericForTest(..) => "GenericForTest",
            Instruction::JumpToForLoop(..) => "JumpToForLoop",
            Instruction::Jump(..) => "Jump",
            Instruction::Call(..) => "Call",
            Instruction::Return(..) => "Return",
        }
    }
}
