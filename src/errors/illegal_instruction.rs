use crate::interpreter::ConstantIndex;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum IllegalInstruction {
    MissingReturnCount,
    MissingArgCount,
    MissingVariadicCount,
    ExpectingConstant,
    MissingByteStringConstant(ConstantIndex),
    MissingNumberConstant(ConstantIndex),
    MissingFunctionConstant(ConstantIndex),
    InvalidHeapKey,
    UnexpectedConstant,
}
