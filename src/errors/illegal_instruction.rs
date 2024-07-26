use crate::interpreter::ConstantIndex;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum IllegalInstruction {
    MissingReturnCount,
    MissingArgCount,
    MissingVariadicCount,
    ExpectingConstant,
    MissingByteStringConstant(ConstantIndex),
    MissingNumberConstant(ConstantIndex),
    MissingFunctionConstant(ConstantIndex),
    UnexpectedConstant,
}

impl std::fmt::Display for IllegalInstruction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IllegalInstruction::MissingReturnCount => write!(f, "missing return count"),
            IllegalInstruction::MissingArgCount => write!(f, "missing argument count"),
            IllegalInstruction::MissingVariadicCount => write!(f, "missing variadic count"),
            IllegalInstruction::ExpectingConstant => write!(f, "expecting Instruction::Constant"),
            IllegalInstruction::MissingByteStringConstant(_) => {
                write!(f, "missing string constant")
            }
            IllegalInstruction::MissingNumberConstant(_) => write!(f, "missing number constant"),
            IllegalInstruction::MissingFunctionConstant(_) => {
                write!(f, "missing function constant")
            }
            IllegalInstruction::UnexpectedConstant => write!(f, "unexpected Instruction::Constant"),
        }
    }
}
