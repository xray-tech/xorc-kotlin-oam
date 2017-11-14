package io.xorc.oam

sealed class Op
data class OpParallel(val left: Int, val right: Int) : Op()
data class OpOtherwise(val left: Int, val right: Int) : Op()
data class OpPruning(val left: Int, val index: Int?, val right: Int) : Op()
data class OpSequential(val left: Int, val index: Int?, val right: Int) : Op()
data class OpCall(val target: CallTarget, val args: List<Int>) : Op()
data class OpTailCall(val target: CallTarget, val args: List<Int>) : Op()
data class OpCoeffect(val index: Int) : Op()
object OpStop : Op()
data class OpConst(val const: Any?) : Op()
data class OpClosure(val pc: Int, val toCopy: Int) : Op()
data class OpLabel(val pc: Int) : Op()
data class OpFFC(val target: Int, val args: List<Int>) : Op()

sealed class CallTarget
data class TFun(val pc: Int) : CallTarget()
data class TDynamic(val index: Int) : CallTarget()

object ConstSignal

data class Pc(val pc: Int, val epc: Int)

sealed class Frame
data class FPruning(var instances: Int, val pending: Pending) : Frame()
data class FOtherwise(var first_value: Boolean, var instances: Int, val otherwise: Pc) : Frame()
data class FSequential(var index: Int?, val right: Pc) : Frame()
data class FCall(val env: Env) : Frame()
object FResult : Frame()

data class Pending(var value: PendValue, val waiters: MutableList<Token>)

sealed class PendValue
data class PendVal(val v: Any?) : PendValue()
object PendStopped : PendValue()
object Pend : PendValue()

typealias Stack = Cons<Frame>

sealed class EnvVal
data class EnvValue(val v: Any?) : EnvVal()
data class EnvPending(val p: Pending) : EnvVal()
typealias Env = MutableList<EnvVal>

data class Token(val pc: Pc, val env: Env, val stack: Stack) {
    override fun toString() = "#Token"
}

data class CodeFun(val envSize: Int, val code: List<Op>)

data class BC(val ops: List<CodeFun>, val ffc: List<String>)

data class Block(val id: Int, val token: Token)
data class Instance(var currentCoeffect: Int, val blocks: MutableList<Block>) {
    fun isRunning() = blocks.size > 0
}

data class Closure(val pc: Int, val toCopy: Int, val env: Env) {
    override fun toString() = "#Closure"
}
data class Label(val pc: Int)
data class Tuple(val v: List<Any?>)
data class Ref(var v: Any?)

object PrimHalt
object PrimUnsupported

typealias Prim = (List<Any?>) -> Any?
typealias Prims = List<Prim>

data class Coeffect(val id: Int, val description: Any?)

data class Snapshot(val values: List<Any?>,
                    val coeffects: List<Coeffect>,
                    val killedCoeffects: List<Int>,
                    val state: Instance)

data class Result(val values: List<Any?>,
                  val coeffects: List<Coeffect>,
                  val killedCoeffects: List<Int>)

data class Cons<out T>(val value: T, val next: Cons<T>?) : Iterable<T> {
    operator override fun iterator(): Iterator<T> {
        var current: Cons<T>? = this
        return object : Iterator<T> {
            operator override fun next() : T {
                val v = current!!.value
                current = current!!.next
                return v
            }

            operator override fun hasNext() = current != null
        }
    }
}
