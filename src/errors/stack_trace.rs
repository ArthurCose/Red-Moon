use std::rc::Rc;

#[derive(Debug, PartialEq)]
pub struct StackTraceFrame {
    pub(crate) source_name: Rc<str>,
    pub(crate) line: usize,
    pub(crate) col: usize,
    pub(crate) instruction_index: usize,
}

impl StackTraceFrame {
    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    pub fn line_and_col(&self) -> (usize, usize) {
        (self.line, self.col)
    }

    pub fn line(&self) -> usize {
        self.line
    }

    pub fn col(&self) -> usize {
        self.col
    }

    pub fn instruction_index(&self) -> usize {
        self.instruction_index
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct StackTrace {
    frames: Vec<StackTraceFrame>,
}

impl StackTrace {
    pub fn push_frame(&mut self, frame: StackTraceFrame) {
        self.frames.push(frame);
    }

    pub fn frames(&self) -> &[StackTraceFrame] {
        &self.frames
    }
}
