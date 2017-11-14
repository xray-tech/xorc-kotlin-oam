package io.xorc.oam

import org.msgpack.core.MessagePack
import org.msgpack.core.MessageUnpacker
import org.msgpack.value.Value as MsgValue
import org.msgpack.value.ValueFactory
import java.io.InputStream

fun deserializeConst(decoder: MessageUnpacker): Any? {
    val next = decoder.unpackValue()
    return when {
        next.isIntegerValue -> next.asIntegerValue().asLong()
        next.isFloatValue -> next.asFloatValue().toDouble()
        next.isStringValue -> next.asStringValue().asString()
        next.isExtensionValue -> {
            val ext = next.asExtensionValue()
            val signal: Byte = 1
            if (ext.type == signal) ConstSignal else throw RuntimeException()
        }
        next.isNilValue -> null
        next.isBooleanValue -> next.asBooleanValue().boolean
        else -> throw RuntimeException()
    }
}


class BCDeserializer(private val decoder: MessageUnpacker) {
    constructor(stream: InputStream) : this(MessagePack.newDefaultUnpacker(stream))

    fun deserialize(): BC {
        decoder.unpackArrayHeader()
        val ffcSize = decoder.unpackArrayHeader()
        val ffc = List(ffcSize) {
            decoder.unpackString()
        }

        val codeSize = decoder.unpackArrayHeader()

        val ops = List(codeSize) {
            decoder.unpackArrayHeader()
            val envSize = decoder.unpackInt()
            val opsSize = decoder.unpackInt()
            val ops = List(opsSize) {
                val type = decoder.unpackInt()
                when (type) {
                    0 -> OpParallel(decoder.unpackInt(), decoder.unpackInt())
                    1 -> OpOtherwise(decoder.unpackInt(), decoder.unpackInt())
                    2 -> {
                        val left = decoder.unpackInt()
                        val index = decoder.unpackInt()
                        OpPruning(left, if (index == -1) null else index, decoder.unpackInt())
                    }
                    3 -> {
                        val left = decoder.unpackInt()
                        val index = decoder.unpackInt()
                        OpSequential(left, if (index == -1) null else index, decoder.unpackInt())
                    }
                    4, 5 -> {
                        val target = when (decoder.unpackInt()) {
                            0 -> TFun(decoder.unpackInt())
                            1 -> TDynamic(decoder.unpackInt())
                            else -> throw RuntimeException()
                        }
                        val argsSize = decoder.unpackArrayHeader()
                        val args = List(argsSize) { decoder.unpackInt() }
                        if (type == 4) OpCall(target, args) else OpTailCall(target, args)
                    }
                    6 -> OpCoeffect(decoder.unpackInt())
                    7 -> OpStop
                    8 -> {
                        OpConst(deserializeConst(decoder))
                    }
                    9 -> OpClosure(decoder.unpackInt(), decoder.unpackInt())
                    10 -> OpLabel(decoder.unpackInt())
                    11 -> {
                        val target = decoder.unpackInt()
                        val argsSize = decoder.unpackArrayHeader()
                        val args = List(argsSize) { decoder.unpackInt() }
                        OpFFC(target, args)
                    }
                    else -> throw RuntimeException()
                }
            }
            CodeFun(envSize, ops)
        }

        return BC(ops, ffc)
    }
}

fun serializeConst(const: Any?): MsgValue {
    if (const == null) {
        return ValueFactory.newNil()
    }
    else {
        return when (const) {
            is Int -> ValueFactory.newInteger(const)
            is Long -> ValueFactory.newInteger(const)
            is Float -> ValueFactory.newFloat(const)
            is Double -> ValueFactory.newFloat(const)
            is Number -> ValueFactory.newFloat(const.toDouble())
            is String -> ValueFactory.newString(const)
            is ConstSignal -> ValueFactory.newExtension(1, "".toByteArray())
            is Boolean -> ValueFactory.newBoolean(const)
            else -> throw RuntimeException()
        }
    }
}

fun serializeSimplValue(v: Any?): List<MsgValue> {
    return when (v) {
        is Tuple -> listOf(
                ValueFactory.newInteger(2),
                ValueFactory.newArray(v.v.flatMap { serializeSimplValue(it) }))
        is List<*> -> listOf(
                ValueFactory.newInteger(3),
                ValueFactory.newArray(v.flatMap { serializeSimplValue(it) }))
        is Map<*,*> -> listOf(
                ValueFactory.newInteger(4),
                ValueFactory.newArray(v.flatMap { listOf(ValueFactory.newString(it.key as String), *serializeSimplValue(it.value).toTypedArray()) }))
        is Label -> listOf(
                ValueFactory.newInteger(5),
                ValueFactory.newInteger(v.pc))
        is Ref -> serializeSimplValue(v.v)
        else -> listOf(
                ValueFactory.newInteger(0),
                serializeConst(v))
    }
}

fun deserializeSimpleValue(decoder: MessageUnpacker): Pair<Int, Any?> {
    return when (decoder.unpackInt()) {
        0 -> Pair(2, deserializeConst(decoder))
        2 -> {
            var size = decoder.unpackArrayHeader()
            val l = mutableListOf<Any?>()
            while (size > 0) {
                val (s, v) = deserializeSimpleValue(decoder)
                size -= s
                l.add(v)
            }
            Pair(2, Tuple(l))
        }
        3 -> {
            var size = decoder.unpackArrayHeader()
            val l = mutableListOf<Any?>()
            while (size > 0) {
                val (s, v) = deserializeSimpleValue(decoder)
                size -= s
                l.add(v)
            }
            Pair(2, l)
        }
        4 -> {
            var size = decoder.unpackArrayHeader()
            val m = mutableMapOf<String, Any?>()
            while (size > 0) {
                val k = decoder.unpackString()
                size -= 1
                val (s, v) = deserializeSimpleValue(decoder)
                size -= s
                m.set(k, v)
            }
            Pair(2, m)
        }
        5 -> {
            val pc = decoder.unpackInt()
            Pair(2, Label(pc))
        }
        else -> throw RuntimeException()
    }
}

class Serializer(val instance: Instance) {
    private val encoder = MessagePack.newDefaultBufferPacker()

    private val frames = mutableListOf<Pair<Int, Frame>>()
    private val envs = mutableListOf<Pair<Int, Env>>()
    private val refs = mutableListOf<Pair<Int, Ref>>()
    private val pendings = mutableListOf<Pair<Int, Pending>>()
    private var id = 0

    private fun <T> in_cache(cache: MutableList<Pair<Int, T>>, v: T): Pair<Int, Boolean> {
        val exists = cache.find { (_, vInCache) -> v === vInCache }
        if (exists == null) {
            id += 1
            cache.add(Pair(id, v))
            return Pair(id, true)
        } else {
            return Pair(exists.first, false)
        }
    }

    private fun <T> newInCache(cache: MutableList<Pair<Int, T>>, v: T): Boolean {
        val (_, isNew) = in_cache(cache, v)
        return isNew
    }

    private fun <T> cacheId(cache: MutableList<Pair<Int, T>>, v: T): Int {
        val (id, isNew) = in_cache(cache, v)
        assert(!isNew)
        return id
    }

    private fun walkFrame(frame: Frame) {
        if (!newInCache(frames, frame)) {
            return
        }
        when (frame) {
            is FPruning -> walkPending(frame.pending)
            is FCall -> walkEnv(frame.env)
        }
    }

    private fun walkStack(stack: Stack) {
        stack.forEach { walkFrame(it) }
    }

    private fun walkPending(p: Pending) {
        if (!newInCache(pendings, p)) {
            return
        }
        p.value.let {
            when (it) {
                is PendVal -> walkV(it.v)
                else -> {
                }
            }
        }
        p.waiters.forEach { walkToken(it) }
    }

    private fun walkRef(ref: Ref) {
        if (!newInCache(refs, ref)) {
            return
        }
        walkV(ref.v)
    }

    private fun walkV(v: Any?) {
        when (v) {
            is Closure -> walkEnv(v.env)
            is Ref -> walkRef(v)
            is Pending -> walkPending(v)
            is Tuple -> v.v.map { walkV(it) }
            is List<*> -> v.map { walkV(it) }
            is Map<*,*> -> v.map { (_, v) -> walkV(v) }
            else -> {
            }
        }
    }

    private fun walkEnv(env: Env) {
        if (!newInCache(envs, env)) {
            return
        }
        for (v in env) {
            when (v) {
                is EnvPending -> walkPending(v.p)
                is EnvValue -> walkV(v.v)
            }
        }
    }

    private fun walkToken(block: Token) {
        walkStack(block.stack)
        walkEnv(block.env)
    }

    private fun serializeFrame(id: Int, frame: Frame): List<MsgValue> {
        val res = mutableListOf<MsgValue>(ValueFactory.newInteger(id))
        when (frame) {
            is FPruning -> {
                res.add(ValueFactory.newInteger(0))
                res.add(ValueFactory.newInteger(frame.instances))
                res.add(ValueFactory.newInteger(cacheId(pendings, frame.pending)))

            }
            is FOtherwise -> {
                res.add(ValueFactory.newInteger(1))
                res.add(ValueFactory.newBoolean(frame.first_value))
                res.add(ValueFactory.newInteger(frame.instances))
                res.add(ValueFactory.newInteger(frame.otherwise.pc))
                res.add(ValueFactory.newInteger(frame.otherwise.epc))
            }
            is FSequential -> {
                res.add(ValueFactory.newInteger(2))
                frame.index.let {
                    if (it == null) {
                        res.add(ValueFactory.newInteger(-1))
                    } else {
                        res.add(ValueFactory.newInteger(it))
                    }
                }
                res.add(ValueFactory.newInteger(frame.right.pc))
                res.add(ValueFactory.newInteger(frame.right.epc))
            }
            is FCall -> {
                res.add(ValueFactory.newInteger(3))
                res.add(ValueFactory.newInteger(cacheId(envs, frame.env)))
            }
            is FResult -> {
                res.add(ValueFactory.newInteger(4))
            }
        }
        return res
    }

    private fun serializeToken(v: Token): MsgValue {
        return ValueFactory.newArray(
                ValueFactory.newInteger(v.pc.pc),
                ValueFactory.newInteger(v.pc.epc),
                ValueFactory.newArray(v.stack.map {
                    ValueFactory.newInteger(cacheId(frames, it))
                }),
                ValueFactory.newInteger(cacheId(envs, v.env)))
    }

    private fun serializePending(id: Int, pending: Pending): List<MsgValue> {
        val res = mutableListOf<MsgValue>(ValueFactory.newInteger(id))
        pending.value.let {
            when (it) {
                is Pend -> {
                    res.add(ValueFactory.newInteger(0))
                    res.add(ValueFactory.newArray(pending.waiters.map { serializeToken(it) }))
                }
                is PendVal -> {
                    res.add(ValueFactory.newInteger(1))
                    for (d in serializeValue(it.v)) {
                        res.add(d)
                    }
                }
                is PendStopped -> {
                    res.add(ValueFactory.newInteger(2))
                }
            }
        }
        return res
    }

    private fun serializeRef(id: Int, ref: Ref): List<MsgValue> {
        return listOf(ValueFactory.newInteger(id),
                *serializeValue(ref.v).toTypedArray())
    }

    private fun serializeEnv(id: Int, env: Env): List<MsgValue> {
        val values = env.map {
            when (it) {
                is EnvPending -> ValueFactory.newArray(
                        ValueFactory.newInteger(1),
                        ValueFactory.newInteger(cacheId(pendings, it.p)))
                is EnvValue -> ValueFactory.newArray(
                        ValueFactory.newInteger(0),
                        *serializeValue(it.v).toTypedArray())

            }
        }
        return listOf(
                ValueFactory.newInteger(id),
                ValueFactory.newArray(values))
    }

    private fun serializeValue(v: Any?): List<MsgValue> {
        return when (v) {
            is Closure -> listOf(
                    ValueFactory.newInteger(1),
                    ValueFactory.newInteger(v.pc),
                    ValueFactory.newInteger(v.toCopy),
                    ValueFactory.newInteger(cacheId(envs, v.env)))
            is Tuple -> listOf(
                    ValueFactory.newInteger(2),
                    ValueFactory.newArray(v.v.flatMap { serializeValue(it) }))
            is List<*> -> listOf(
                    ValueFactory.newInteger(3),
                    ValueFactory.newArray(v.flatMap { serializeValue(it) }))
            is Map<*,*> -> listOf(
                    ValueFactory.newInteger(4),
                    ValueFactory.newArray(v.flatMap { listOf(ValueFactory.newString(it.key as String), *serializeValue(it.value).toTypedArray()) }))
            is Label -> listOf(
                    ValueFactory.newInteger(5),
                    ValueFactory.newInteger(v.pc))
            is Ref -> listOf(
                    ValueFactory.newInteger(6),
                    ValueFactory.newInteger(cacheId(refs, v)))
            is Pending -> listOf(
                    ValueFactory.newInteger(7),
                    ValueFactory.newInteger(cacheId(pendings, v)))
            else -> listOf(
                    ValueFactory.newInteger(0),
                    serializeConst(v))
        }
    }

    private fun serializeBlock(block: Block): List<MsgValue> {
        return listOf(
                ValueFactory.newInteger(block.id),
                serializeToken(block.token))
    }

    fun serialize(): ByteArray {
        instance.blocks.forEach { walkToken(it.token) }
        encoder.packArrayHeader(5)
        encoder.packInt(instance.currentCoeffect)
        val framesData = frames.flatMap { (id, frame) -> serializeFrame(id, frame) }
        encoder.packArrayHeader(framesData.size)
        framesData.forEach { encoder.packValue(it) }

        val pendingsData = pendings.flatMap { (id, pending) -> serializePending(id, pending) }
        encoder.packArrayHeader(pendingsData.size)
        pendingsData.forEach { encoder.packValue(it) }

        val refsData = refs.flatMap { (id, ref) -> serializeRef(id, ref) }
        encoder.packArrayHeader(refsData.size)
        refsData.forEach { encoder.packValue(it) }

        val envsData = envs.flatMap { (id, env) -> serializeEnv(id, env) }
        encoder.packArrayHeader(envsData.size)
        envsData.forEach { encoder.packValue(it) }

        val blocksData = instance.blocks.flatMap { block -> serializeBlock(block) }
        encoder.packArrayHeader(blocksData.size)
        blocksData.forEach { encoder.packValue(it) }

        return encoder.toByteArray()
    }
}

class ResultSerializer(val res: Result) {
    private val encoder = MessagePack.newDefaultBufferPacker()

    fun serialize(): ByteArray {
        encoder.packArrayHeader(3)

        encoder.packArrayHeader(res.values.size)
        for (v in res.values) {
            val serialized = serializeSimplValue(v)
            encoder.packArrayHeader(serialized.size)
            for (part in serialized) {
                encoder.packValue(part)
            }
        }

        encoder.packArrayHeader(res.coeffects.size)
        for (v in res.coeffects) {
            val parts = serializeSimplValue(v.description)
            encoder.packArrayHeader(parts.size + 1)
            encoder.packInt(v.id)
            for (part in parts) {
                encoder.packValue(part)
            }
        }

        encoder.packArrayHeader(res.killedCoeffects.size)
        for (v in res.killedCoeffects) {
            encoder.packInt(v)
        }

        return encoder.toByteArray()
    }
}

class Deserializer(private val decoder: MessageUnpacker) {

    constructor(stream: InputStream) : this(MessagePack.newDefaultUnpacker(stream))

    private val frames = mutableMapOf<Int, Frame>()
    private val envs = mutableMapOf<Int, Env>()
    private val pendings = mutableMapOf<Int, Pending>()
    private val refs = mutableMapOf<Int, Ref>()

    private fun cachedOrDummyPending(id: Int) = pendings.getOrPut(id) {
        Pending(Pend, mutableListOf())
    }

    private fun cachedOrDummyEnv(id: Int) = envs.getOrPut(id) {
        mutableListOf()
    }

    private fun cachedOrDummyRef(id: Int) = refs.getOrPut(id) {
        Ref(null)
    }

    private fun frames() {
        var framesSize = decoder.unpackArrayHeader()
        while (framesSize > 0) {
            val id = decoder.unpackInt()
            val type = decoder.unpackInt()
            framesSize -= 2
            when (type) {
                0 -> {
                    val instances = decoder.unpackInt()
                    val pending = decoder.unpackInt()
                    framesSize -= 2
                    frames.put(id, FPruning(instances, cachedOrDummyPending(pending)))
                }
                1 -> {
                    val firstValue = decoder.unpackBoolean()
                    val instances = decoder.unpackInt()
                    val pc = decoder.unpackInt()
                    val epc = decoder.unpackInt()
                    framesSize -= 4
                    frames.put(id, FOtherwise(firstValue, instances, Pc(pc, epc)))
                }
                2 -> {
                    val index = decoder.unpackInt()
                    val pc = decoder.unpackInt()
                    val epc = decoder.unpackInt()
                    framesSize -= 3
                    frames.put(id, FSequential(if (index == -1) null else index, Pc(pc, epc)))
                }
                3 -> {
                    val env = decoder.unpackInt()
                    framesSize -= 1
                    frames.put(id, FCall(cachedOrDummyEnv(env)))
                }
                4 -> frames.put(id, FResult)
            }
        }
    }

    private fun deserializeToken(): Token {
        decoder.unpackArrayHeader()
        val pc = decoder.unpackInt()
        val epc = decoder.unpackInt()
        var size = decoder.unpackArrayHeader()
        fun decodeStack(): Stack? {
            return if (size > 0) {
                size--
                Cons(frames.get(decoder.unpackInt())!!, decodeStack())
            } else {
                null
            }
        }
        val stack = decodeStack()!!
        val env = decoder.unpackInt()
        return Token(Pc(pc, epc), cachedOrDummyEnv(env), stack)
    }

    private fun deserializeTokens(): MutableList<Token> {
        val size = decoder.unpackArrayHeader()
        return MutableList(size) {
            deserializeToken()
        }
    }

    private fun pendings() {
        var pendingsSize = decoder.unpackArrayHeader()
        while (pendingsSize > 0) {
            val id = decoder.unpackInt()
            val type = decoder.unpackInt()
            val p = cachedOrDummyPending(id)
            pendingsSize -= 2
            when (type) {
                0 -> {
                    val tokens = deserializeTokens()
                    pendingsSize -= 1
                    p.waiters.addAll(tokens)
                }
                1 -> {
                    val (size, v) = deserializeValue(decoder)
                    pendingsSize -= size
                    p.value = PendVal(v)
                                    }
                2 -> {
                    p.value = PendStopped
                }
                else -> throw RuntimeException()
            }
            pendings.put(id, p)
        }
    }

    private fun refs() {
        var refsSize = decoder.unpackArrayHeader()
        while (refsSize > 0) {
            val id = decoder.unpackInt()
            refsSize -= 1
            val (size, v) = deserializeValue(decoder)
            refsSize -= size
            val ref = cachedOrDummyRef(id)
            ref.v = v
            refs.put(id, ref)
        }
    }

    private fun deserializeValue(decoder: MessageUnpacker): Pair<Int, Any?> {
        return when (decoder.unpackInt()) {
            0 -> Pair(2, deserializeConst(decoder))
            1 -> {
                val pc = decoder.unpackInt()
                val toCopy = decoder.unpackInt()
                val env = decoder.unpackInt()
                Pair(4, Closure(pc, toCopy, cachedOrDummyEnv(env)))
            }
            2 -> {
                var size = decoder.unpackArrayHeader()
                val l = mutableListOf<Any?>()
                while (size > 0) {
                    val (s, v) = deserializeValue(decoder)
                    size -= s
                    l.add(v)
                }
                Pair(2, Tuple(l))
            }
            3 -> {
                var size = decoder.unpackArrayHeader()
                val l = mutableListOf<Any?>()
                while (size > 0) {
                    val (s, v) = deserializeValue(decoder)
                    size -= s
                    l.add(v)
                }
                Pair(2, l)
            }
            4 -> {
                var size = decoder.unpackArrayHeader()
                val m = mutableMapOf<String, Any?>()
                while (size > 0) {
                    val k = decoder.unpackString()
                    size -= 1
                    val (s, v) = deserializeValue(decoder)
                    size -= s
                    m.set(k, v)
                }
                Pair(2, m)
            }
            5 -> {
                val pc = decoder.unpackInt()
                Pair(2, Label(pc))
            }
            6 -> {
                val refId = decoder.unpackInt()
                Pair(2, cachedOrDummyRef(refId))
            }
            7 -> {
                val pendId = decoder.unpackInt()
                Pair(2, cachedOrDummyPending(pendId))
            }
            else -> throw RuntimeException()
        }
    }

    private fun deserializeEnvValue(): EnvVal {
        return when (decoder.unpackInt()) {
            0 -> {
                val (_, v) = deserializeValue(decoder)
                EnvValue(v)
            }
            1 -> {
                EnvPending(cachedOrDummyPending(decoder.unpackInt()))
            }
            else -> throw RuntimeException()
        }
    }

    private fun envs() {
        val envsSize = decoder.unpackArrayHeader()
        for (i in 1..(envsSize / 2)) {
            val id = decoder.unpackInt()
            val valuesSize = decoder.unpackArrayHeader()

            val env = cachedOrDummyEnv(id)
            for (j in 1..valuesSize) {
                decoder.unpackArrayHeader()
                env.add(deserializeEnvValue())
            }
        }
    }

    private fun blocks(): MutableList<Block> {
        val blocksSize = decoder.unpackArrayHeader()
        return MutableList(blocksSize / 2) {
            Block(decoder.unpackInt(), deserializeToken())
        }

    }

    fun deserialize(): Instance {
        assert(decoder.unpackArrayHeader() == 5)
        val currentCoeffect = decoder.unpackInt()
        frames()
        pendings()
        refs()
        envs()
        return Instance(currentCoeffect, blocks())
    }
}
