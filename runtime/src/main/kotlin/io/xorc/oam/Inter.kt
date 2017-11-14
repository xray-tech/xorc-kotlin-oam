package io.xorc.oam

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*

class UnknownCoeffect() : RuntimeException()

class Inter(val bc: BC) {
    val LOG = LoggerFactory.getLogger(this.javaClass.name)

    private var instance: Instance = Instance(0, mutableListOf())
    private var values = mutableListOf<Any?>()
    private var coeffects = mutableListOf<Coeffect>()

    private var queue = LinkedList<Token>()

    private val prims = Context.getInstance().snapshot(bc.ffc)

    sealed class Realized {
        data class Values(val vals: List<Any?>) : Realized()
        data class Pendings(val pendings: List<Pending>) : Realized()
        object Stopped : Realized()
    }

    private fun getCode(pc: Int): CodeFun {
        return bc.ops[pc]
    }

    private fun copyEnv(from: Env, size: Int, offset: Int, args: List<Int>): Env {
        return MutableList(size) { index ->
            if (index < offset) {
                EnvValue(null)
            } else if (index < args.size + offset) {
                from[args[index - offset]]
            } else {
                EnvValue(null)
            }
        }
    }

    private fun copyStack(stack: Stack?) : Stack? {
        if (stack == null) {
            return null
        }
        fun copyFrame(f: Frame) : Frame {
            return when (f) {
                is FCall -> FCall(f.env.toMutableList())
                else -> f
            }
        }
        return Cons(copyFrame(stack.value), copyStack(stack.next))
    }



    private tailrec fun tick(pc: Pc, stack: Stack, env: Env, subscribe: Boolean) {
        val op = getCode(pc.pc).code[pc.epc]
        fun realized(args: List<Int>): Realized {
            val values = mutableListOf<Any?>()
            val pendings = mutableListOf<Pending>()

            for (arg in args) {
                val argV = env[arg]
                when (argV) {
                    is EnvPending -> {
                        argV.p.value.let {
                            when (it) {
                                is Pend -> pendings.add(argV.p)
                                is PendStopped -> {
                                    return Realized.Stopped
                                }
                                is PendVal -> values.add(it.v)
                            }
                        }
                    }
                    is EnvValue -> {
                        values.add(argV.v)
                    }
                }
            }
            if (pendings.size > 0) {
                return Realized.Pendings(pendings)
            } else {
                return Realized.Values(values)
            }

        }

        fun callTuple(tuple: Tuple, args: List<Int>) {
            val index = realized(listOf(args[0]))

            when (index) {
                is Realized.Values -> {
                    val indexV = index.vals[0]
                    when (indexV) {
                        is Int -> publishAndHalt(stack, env, tuple.v[indexV])
                        is Number -> publishAndHalt(stack, env, tuple.v[indexV.toInt()])
                        else ->
                            LOG.warn("Can't get value of tuple by {}", indexV)
                    }
                }
                is Realized.Stopped -> halt(stack, env)
                is Realized.Pendings ->
                    if (subscribe) index.pendings.map { p -> p.waiters.add(Token(pc, env, stack)) }
            }
        }
//        System.err.println("---TICK ${pc.pc}:${pc.epc} ${op} ${env}")
        when (op) {
            is OpConst -> publishAndHalt(stack, env, op.const)
            is OpLabel -> publishAndHalt(stack, env, Label(op.pc))
            is OpStop -> halt(stack, env)
            is OpParallel -> {
                incrementInstances(stack)

                queue.addFirst(Token(Pc(pc.pc, op.right), env.toMutableList(), copyStack(stack)!!))
//                tick(Pc(pc.pc, op.left), stack, newEnv, true)
                tick(Pc(pc.pc, op.left), stack, env, true)
            }
            is OpOtherwise -> {
                val frame = FOtherwise(false, 1, Pc(pc.pc, op.right))
                tick(Pc(pc.pc, op.left), Cons(frame, stack), env, true)
            }
            is OpPruning -> {
                val pending = Pending(Pend, mutableListOf())
                val frame = FPruning(1, pending)
                op.index?.let { env.set(it, EnvPending(pending)) }
//                queue.add(Token(Pc(pc.pc, op.left), env, stack))
                tick(Pc(pc.pc, op.right), Cons(frame, stack), env, true)
                tick(Pc(pc.pc, op.left), stack, env, true)
            }
            is OpSequential -> {
                val frame = FSequential(op.index, Pc(pc.pc, op.right))
                tick(Pc(pc.pc, op.left), Cons(frame, stack), env, true)
            }
            is OpClosure -> publishAndHalt(stack, env, Closure(op.pc, op.toCopy, env))
            is OpTailCall -> {
                val t = op.target
                when (t) {
                    is TFun -> {
                        val f = getCode(t.pc)
                        val newEnv = copyEnv(env, f.envSize, 0, op.args)
                        tick(Pc(t.pc, f.code.lastIndex), stack, newEnv, true)
                    }
                    is TDynamic -> {
                        val realized = realized(listOf(t.index))
                        when (realized) {
                            is Realized.Values -> {
                                val v = realized.vals[0]
                                when (v) {
                                    is Closure -> {
                                        val f = getCode(v.pc)
                                        val newEnv = copyEnv(env, f.envSize, v.toCopy, op.args)
                                        for (i in 0..(v.toCopy - 1)) {
                                            newEnv[i] = v.env[i]
                                        }
                                        tick(Pc(v.pc, f.code.lastIndex), stack, newEnv, true)
                                    }
                                    is Label -> {
                                        val f = getCode(v.pc)
                                        val newEnv = copyEnv(env, f.envSize, 0, op.args)
                                        tick(Pc(v.pc, f.code.lastIndex), stack, newEnv, true)
                                    }
                                    is Tuple -> callTuple(v, op.args)
                                }
                            }
                            is Realized.Stopped -> halt(stack, env)
                            is Realized.Pendings ->
                                if (subscribe) realized.pendings.map { p -> p.waiters.add(Token(pc, env, stack)) }
                        }
                    }
                }
            }
            is OpCall -> {
                val t = op.target
                when (t) {
                    is TFun -> {
                        val f = getCode(t.pc)
                        val newEnv = copyEnv(env, f.envSize, 0, op.args)
                        val frame = FCall(env)
                        tick(Pc(t.pc, f.code.lastIndex), Cons(frame, stack), newEnv, true)
                    }
                    is TDynamic -> {
                        val realized = realized(listOf(t.index))
                        when (realized) {
                            is Realized.Values -> {
                                val v = realized.vals[0]
                                when (v) {
                                    is Closure -> {
                                        val f = getCode(v.pc)
                                        val newEnv = copyEnv(env, f.envSize, v.toCopy, op.args)
                                        for (i in 0..(v.toCopy - 1)) {
                                            newEnv[i] = v.env[i]
                                        }
                                        val frame = FCall(env)
                                        tick(Pc(v.pc, f.code.lastIndex), Cons(frame, stack), newEnv, true)
                                    }
                                    is Label -> {
                                        val f = getCode(v.pc)
                                        val newEnv = copyEnv(env, f.envSize, 0, op.args)
                                        val frame = FCall(env)
                                        tick(Pc(v.pc, f.code.lastIndex), Cons(frame, stack), newEnv, true)
                                    }
                                    is Tuple -> callTuple(v, op.args)
                                }
                            }
                            is Realized.Stopped -> halt(stack, env)
                            is Realized.Pendings ->
                                if (subscribe) realized.pendings.map { p -> p.waiters.add(Token(pc, env, stack)) }
                        }
                    }
                }
            }
            is OpCoeffect -> {
                val realized = realized(listOf(op.index))
                when (realized) {
                    is Realized.Values -> {
                        val v = realized.vals[0]
                        val id = instance.currentCoeffect
                        instance.currentCoeffect += 1
                        instance.blocks.add(Block(id, Token(pc, env.toMutableList(), stack)))
                        coeffects.add(Coeffect(id, v))
                    }
                    is Realized.Stopped -> halt(stack, env)
                    is Realized.Pendings ->
                        if (subscribe) realized.pendings.map { p -> p.waiters.add(Token(pc, env, stack)) }
                }
            }
            is OpFFC -> {
                val impl = prims[op.target]
                val args = realized(op.args)
                when (args) {
                    is Realized.Values -> {
                        fun wrongNumberOfArguments() {
                            LOG.warn("Wrong number of arguments for \"${prims.getName(op.target)}\": ${args.vals.size}}")
                        }

                        fun pendingExpected(v: Any?) {
                            LOG.warn("\"${prims.getName(op.target)}\" expects Pending as argument, actual: {}", v)
                        }
//                        System.err.println("---FFC ${prims.getName(op.target)} ${args.vals}")
                        when (prims.getName(op.target)) {
                            "core.make-pending" -> when (args.vals.size) {
                                0 -> publishAndHalt(stack, env, Pending(Pend, mutableListOf()))
                                else -> wrongNumberOfArguments()
                            }
                            "core.pending-read" -> when (args.vals.size) {
                                1 -> {
                                    val p = args.vals[0]
                                    when {
                                        p is Pending -> {
                                            when (p.value) {
                                                is Pend -> if (subscribe) p.waiters.add(Token(pc, env, stack))
                                                is PendStopped -> halt(stack, env)
                                                is PendVal -> publishAndHalt(stack, env, p.value)
                                            }
                                        }
                                        else -> pendingExpected(p)
                                    }
                                }
                                else -> wrongNumberOfArguments()
                            }
                            "core.realize" -> when (args.vals.size) {
                                2 -> {
                                    val p = args.vals[0]
                                    when {
                                        p is Pending -> {
                                            pendingRealize(p, args.vals[1])
                                            publishAndHalt(stack, env, ConstSignal)
                                        }
                                        else -> pendingExpected(p)
                                    }
                                }
                                else -> wrongNumberOfArguments()
                            }
                            "core.is-realized" -> when (args.vals.size) {
                                1 -> {
                                    val p = args.vals[0]
                                    when {
                                        p is Pending -> publishAndHalt(stack, env, p.value is PendVal)
                                        else -> pendingExpected(p)
                                    }
                                }
                                else -> wrongNumberOfArguments()
                            }
                            "core.stop-pending" -> when (args.vals.size) {
                                1 -> {
                                    val p = args.vals[0]
                                    when (p) {
                                        is Pending -> {
                                            pendingStop(p)
                                            publishAndHalt(stack, env, ConstSignal)
                                        }
                                        else -> pendingExpected(p)
                                    }
                                }
                                else -> wrongNumberOfArguments()
                            }
                            else -> {
                                val res = impl(args.vals)
//                                System.err.println("---FFC RES ${res}")
                                when (res) {
                                    is PrimHalt -> halt(stack, env)
                                    is PrimUnsupported ->
                                        LOG.warn("FFC error ${prims.getName(op.target)}; arguments: {}", args.vals)
                                    else -> publishAndHalt(stack, env, res)
                                }


                            }

                        }
                    }
                    is Realized.Stopped -> halt(stack, env)
                    is Realized.Pendings ->
                        if (subscribe) args.pendings.map { p -> p.waiters.add(Token(pc, env, stack)) }
                }
            }
        }
    }

    private fun pendingRealize(pending: Pending, v: Any?) {
        if (pending.value is PendVal) {
            return
        }
        pending.value = PendVal(v)
        for (w in pending.waiters) {
            if (isAlive(w.stack)) {
                tick(w.pc, w.stack, w.env, false)
            }
        }
    }

    private fun pendingStop(pending: Pending) {
        if (pending.value !is Pend) {
            return
        }
        pending.value = PendStopped
        for (w in pending.waiters) {
            tick(w.pc, w.stack, w.env, false)
        }
    }

    private fun publishAndHalt(stack: Stack, env: Env, v: Any?) {
        publish(stack, env, v)
        halt(stack, env)
    }

    private tailrec fun publish(stack: Stack, env: Env, v: Any?) {
        val s = stack.value
        when (s) {
            is FResult -> values.add(v)
            is FSequential -> {
                incrementInstances(stack)
                s.index?.let { env.set(it, EnvValue(v)) }
                tick(s.right, stack.next!!, env, true)
            }
            is FOtherwise -> {
                s.first_value = true
                publish(stack.next!!, env, v)
            }
            is FPruning -> {
                pendingRealize(s.pending, v)
            }
            is FCall -> {
                publish(stack.next!!, s.env, v)
            }
        }
    }

    private fun halt(stack: Stack, env: Env) {
        val s = stack.value
        when (s) {
            is FResult -> {
            }
            is FOtherwise -> {
                if (!s.first_value && s.instances == 1) {
                    tick(s.otherwise, stack.next!!, env, true)
                } else if (s.instances == 1) {
                    halt(stack.next!!, env)
                } else {
                    s.instances -= 1
                }
            }
            is FPruning -> {
                if (s.pending.value is Pend && s.instances == 1) {
                    pendingStop(s.pending)
                } else {
                    s.instances -= 1
                }
            }
            is FCall -> {
                halt(stack.next!!, s.env)
            }
            is FSequential -> {
                halt(stack.next!!, env)
            }

        }
    }

    private fun incrementInstances(stack: Stack) {
        loop@ for (s in stack) {
            when (s) {
                is FOtherwise -> {
                    s.instances += 1; break@loop
                }
                is FPruning -> {
                    s.instances += 1; break@loop
                }
            }
        }
    }

    private fun isAlive(stack: Stack): Boolean {
        for (s in stack) {
            if (s is FPruning && s.pending.value !is Pend) {
                return false
            }
        }
        return true
    }

    private fun checkKilled(justUnblocked: Int?): List<Int> {
        val killed = mutableListOf<Int>()
        val newBlocks = mutableListOf<Block>()
        for (block in instance.blocks) {
            when {
                block.id == justUnblocked -> {
                }
                !isAlive(block.token.stack) -> {
                    killed.add(block.id)
                }
                else -> newBlocks.add(block)
            }
        }
        instance.blocks.clear()
        instance.blocks.addAll(newBlocks)
        return killed
    }

    private fun runLoop() {
        while (queue.isNotEmpty()) {
            val token = queue.removeFirst()
            tick(token.pc, token.stack, token.env, true)
        }
    }

    fun run(): Snapshot {
        val l = bc.ops[0]
        val pc = Pc(0, l.code.lastIndex)
        val stack = Cons(FResult, null)
        val env: MutableList<EnvVal> = MutableList(l.envSize, { EnvValue(null) })
        values = mutableListOf()
        coeffects = mutableListOf()
        queue.addFirst(Token(pc, env, stack))
        runLoop()
        val killed = checkKilled(null)
        return Snapshot(values, coeffects, killed, instance)
    }

    fun unblock(instance: Instance, id: Int, value: Any?): Snapshot {
        this.instance = instance
        values = mutableListOf()
        coeffects = mutableListOf()
        val block = instance.blocks.find { it.id == id }
        instance.blocks.removeIf { it.id == id }
        if (block == null) {
            throw UnknownCoeffect()
        } else {
            publishAndHalt(block.token.stack, block.token.env, value)
            runLoop()
            val killed = checkKilled(id)
            return Snapshot(values, coeffects, killed, instance)
        }
    }

    fun unblock(id: Int, value: Any?): Snapshot {
        return unblock(instance, id, value)
    }
}
