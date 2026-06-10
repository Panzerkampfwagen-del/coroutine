/* Cooperative context switching, dispatched by architecture. This file is a
 * `.S` source, so the C preprocessor selects the right implementation.
 *
 * Two symbols are exported per architecture:
 *   void coro_context_switch(coro_context_t *from, coro_context_t *to)
 *       Save the callee-saved registers into *from, restore them from *to, and
 *       resume *to. Only callee-saved state is touched; the caller has already
 *       spilled live caller-saved registers around the call.
 *   void coro_trampoline(void)
 *       The first code a fresh coroutine runs: call coro_entry (which runs the
 *       user function) and then fall into coro_exit when it returns.
 */

#if defined(__x86_64__)

/* System V AMD64 ABI, AT&T syntax. coro_context_t offsets:
       0 rbx  8 rbp  16 r12  24 r13  32 r14  40 r15  48 rsp           */

    .text

/* from -> %rdi, to -> %rsi. Swapping rsp means the final `ret` pops the return
   address off *to's stack, resuming where *to last left off. */
    .globl coro_context_switch
    .type  coro_context_switch, @function
    .align 16
coro_context_switch:
    movq %rbx,  0(%rdi)
    movq %rbp,  8(%rdi)
    movq %r12, 16(%rdi)
    movq %r13, 24(%rdi)
    movq %r14, 32(%rdi)
    movq %r15, 40(%rdi)
    movq %rsp, 48(%rdi)

    movq  0(%rsi), %rbx
    movq  8(%rsi), %rbp
    movq 16(%rsi), %r12
    movq 24(%rsi), %r13
    movq 32(%rsi), %r14
    movq 40(%rsi), %r15
    movq 48(%rsi), %rsp

    ret
    .size coro_context_switch, .-coro_context_switch

/* coro_create plants coro_exit just above the trampoline on the stack. We call
   coro_entry; when it returns, the trailing `ret` pops coro_exit and falls into
   it. Routing through `call` keeps coro_entry and coro_exit ABI-aligned. */
    .globl coro_trampoline
    .type  coro_trampoline, @function
    .align 16
coro_trampoline:
    call coro_entry
    ret
    .size coro_trampoline, .-coro_trampoline

    /* Non-executable stack. */
    .section .note.GNU-stack,"",@progbits

#elif defined(__aarch64__)

/* AAPCS64. coro_context_t offsets:
       0..72   x19..x28
       80 fp(x29)  88 lr(x30)  96 sp
       104..160  d8..d15                                              */

    .text

/* from -> x0, to -> x1. lr carries the resume address, so the final `ret`
   branches to *to's saved lr. */
    .globl coro_context_switch
    .type  coro_context_switch, %function
    .align 4
coro_context_switch:
    stp x19, x20, [x0, #0]
    stp x21, x22, [x0, #16]
    stp x23, x24, [x0, #32]
    stp x25, x26, [x0, #48]
    stp x27, x28, [x0, #64]
    stp x29, x30, [x0, #80]      // fp, lr
    mov x2, sp
    str x2, [x0, #96]
    stp d8,  d9,  [x0, #104]
    stp d10, d11, [x0, #120]
    stp d12, d13, [x0, #136]
    stp d14, d15, [x0, #152]

    ldp x19, x20, [x1, #0]
    ldp x21, x22, [x1, #16]
    ldp x23, x24, [x1, #32]
    ldp x25, x26, [x1, #48]
    ldp x27, x28, [x1, #64]
    ldp x29, x30, [x1, #80]
    ldr x2, [x1, #96]
    mov sp, x2
    ldp d8,  d9,  [x1, #104]
    ldp d10, d11, [x1, #120]
    ldp d12, d13, [x1, #136]
    ldp d14, d15, [x1, #152]

    ret
    .size coro_context_switch, .-coro_context_switch

/* No return address is planted on the stack; the trampoline calls coro_entry
   and tail-branches to coro_exit when it returns. */
    .globl coro_trampoline
    .type  coro_trampoline, %function
    .align 4
coro_trampoline:
    bl coro_entry
    b  coro_exit
    .size coro_trampoline, .-coro_trampoline

    /* Non-executable stack. */
    .section .note.GNU-stack,"",%progbits

#else
#error "context.S: unsupported architecture (x86-64 and aarch64 only)"
#endif
