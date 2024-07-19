use super::heap::{GarbageCollector, Heap};
use super::StringRef;

pub struct MetatableKeys {
    /// Length `#`
    pub len: StringRef,
    /// Unary Minus `-`
    pub unm: StringRef,
    /// Bitwise Not `~`
    pub bnot: StringRef,
    /// Add `+`
    pub add: StringRef,
    /// Subtract `-`
    pub sub: StringRef,
    /// Multiply `*`
    pub mul: StringRef,
    /// Division `/`
    pub div: StringRef,
    /// Integer Division `//`
    pub idiv: StringRef,
    /// Modulus `%`
    pub modulus: StringRef,
    /// Power `^`
    pub pow: StringRef,
    /// Bitwise And `&`
    pub band: StringRef,
    /// Bitwise Or `|`
    pub bor: StringRef,
    /// Bitwise Xor `~`
    pub bxor: StringRef,
    /// Bit Shift Left `<<`
    pub shl: StringRef,
    /// Bit Shift Right `>>`
    pub shr: StringRef,
    /// Equal `==`
    pub eq: StringRef,
    /// Less Than `<`
    pub lt: StringRef,
    /// Less Than Equal `<=`
    pub le: StringRef,
    /// Concat `..`
    pub concat: StringRef,
    /// Index `table[key]`
    pub index: StringRef,
    /// Table Assignment `table[key] =`
    pub newindex: StringRef,
    /// Call `table(...)`
    pub call: StringRef,
    /// Mode, "k" for weak keys, "v" for weak values, or "kv" for both
    pub mode: StringRef,
    /// Close, called when the value dropped
    pub close: StringRef,
    /// Called when the table is about to be collected
    pub gc: StringRef,
    /// Prevents metatable changes when set
    pub metatable: StringRef,
    /// Just a string, used by tostring and errors
    pub name: StringRef,
    /// Function, used by tostring and errors
    pub tostring: StringRef,
    /// Override behavior for pairs
    pub pairs: StringRef,
    /// Override behavior for ipairs
    pub ipairs: StringRef,
}

impl MetatableKeys {
    pub(crate) fn new(gc: &mut GarbageCollector, heap: &mut Heap) -> Self {
        Self {
            len: StringRef(heap.intern_bytes_to_ref(gc, b"__len")),
            unm: StringRef(heap.intern_bytes_to_ref(gc, b"__unm")),
            bnot: StringRef(heap.intern_bytes_to_ref(gc, b"__bnot")),
            add: StringRef(heap.intern_bytes_to_ref(gc, b"__add")),
            sub: StringRef(heap.intern_bytes_to_ref(gc, b"__sub")),
            mul: StringRef(heap.intern_bytes_to_ref(gc, b"__mul")),
            div: StringRef(heap.intern_bytes_to_ref(gc, b"__div")),
            idiv: StringRef(heap.intern_bytes_to_ref(gc, b"__idiv")),
            modulus: StringRef(heap.intern_bytes_to_ref(gc, b"__mod")),
            pow: StringRef(heap.intern_bytes_to_ref(gc, b"__pow")),
            band: StringRef(heap.intern_bytes_to_ref(gc, b"__band")),
            bor: StringRef(heap.intern_bytes_to_ref(gc, b"__bor")),
            bxor: StringRef(heap.intern_bytes_to_ref(gc, b"__bxor")),
            shl: StringRef(heap.intern_bytes_to_ref(gc, b"__shl")),
            shr: StringRef(heap.intern_bytes_to_ref(gc, b"__shr")),
            eq: StringRef(heap.intern_bytes_to_ref(gc, b"__eq")),
            lt: StringRef(heap.intern_bytes_to_ref(gc, b"__lt")),
            le: StringRef(heap.intern_bytes_to_ref(gc, b"__le")),
            concat: StringRef(heap.intern_bytes_to_ref(gc, b"__concat")),
            index: StringRef(heap.intern_bytes_to_ref(gc, b"__index")),
            newindex: StringRef(heap.intern_bytes_to_ref(gc, b"__newindex")),
            call: StringRef(heap.intern_bytes_to_ref(gc, b"__call")),
            mode: StringRef(heap.intern_bytes_to_ref(gc, b"__mode")),
            close: StringRef(heap.intern_bytes_to_ref(gc, b"__close")),
            gc: StringRef(heap.intern_bytes_to_ref(gc, b"__gc")),
            metatable: StringRef(heap.intern_bytes_to_ref(gc, b"__metatable")),
            name: StringRef(heap.intern_bytes_to_ref(gc, b"__name")),
            tostring: StringRef(heap.intern_bytes_to_ref(gc, b"__tostring")),
            pairs: StringRef(heap.intern_bytes_to_ref(gc, b"__pairs")),
            ipairs: StringRef(heap.intern_bytes_to_ref(gc, b"__ipairs")),
        }
    }
}
