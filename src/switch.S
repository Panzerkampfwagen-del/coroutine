/*
 * coro_swap(coro_ctx_t *from = rdi, coro_ctx_t *to = rsi)
 *
 * Saves only the callee-saved registers -- caller-saved registers are dead
 * across a cooperative yield by definition. Each register is pushed onto the
 * outgoing coroutine's own stack, so saving %rsp captures the whole context.
 * Restoring is the exact inverse followed by ret.
 */
    .text
    .globl coro_swap
    .type  coro_swap, @function
coro_swap:
    push %rbp
    push %rbx
    push %r12
    push %r13
    push %r14
    push %r15
    mov  %rsp, (%rdi)        # from->rsp = current top of frame
    mov  (%rsi), %rsp        # adopt target's saved frame
    pop  %r15
    pop  %r14
    pop  %r13
    pop  %r12
    pop  %rbx
    pop  %rbp
    ret                      # resume where the target last yielded
    .size coro_swap, .-coro_swap
