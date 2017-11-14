package io.xorc.oam

import com.beust.klaxon.*


fun primsLoad() {
    val c = Context.getInstance()

    c["core.noop"] = { _ -> ConstSignal }
    c["core.let"] = { values ->
        when (values.size) {
            0 -> ConstSignal
            1 -> values[0]
            else -> Tuple(values)
        }
    }
    c["core.add"] = { values ->
        when (values.size) {
            2 -> {
                val a = values[0]
                val b = values[1]
                when {
                    a is String -> a + b
                    a is Map<*, *> && b is Map<*, *> -> a.plus(b)
                    a is Number && b is Number -> Numbers.add(a, b)
                    else -> PrimUnsupported
                }

            }
            else -> PrimUnsupported
        }
    }

    c["core.sub"] = { values ->
        when (values.size) {
            1 -> {
                val a = values[0]
                when (a) {
                    is Number -> Numbers.minus(a)
                    else -> PrimUnsupported
                }
            }
            2 -> {
                val a = values[0]
                val b = values[1]
                when {
                    a is Number && b is Number -> Numbers.minus(a, b)
                    else -> PrimUnsupported
                }
            }
            else -> PrimUnsupported
        }
    }

    c["core.ift"] = { values ->
        when (values.size) {
            1 -> {
                val a = values[0]
                when (a) {
                    is Boolean -> if (a) ConstSignal else PrimHalt
                    else -> PrimUnsupported
                }
            }
            else -> PrimUnsupported
        }
    }

    c["core.iff"] = { values ->
        when (values.size) {
            1 -> {
                val a = values[0]
                when (a) {
                    is Boolean -> if (!a) ConstSignal else PrimHalt
                    else -> PrimUnsupported
                }
            }
            else -> PrimUnsupported
        }
    }

    c["core.mult"] = { values ->
        when (values.size) {
            2 -> {
                val a = values[0]
                val b = values[1]
                when {
                    a is Int && b is Int -> a * b
                    a is Long && b is Long -> a * b
                    a is Number && b is Number -> a.toFloat() * b.toFloat()
                    else -> PrimUnsupported
                }
            }
            else -> PrimUnsupported
        }
    }

    c["core.div"] = { values ->
        when (values.size) {
            2 -> {
                val a = values[0]
                val b = values[1]
                when {
                    a is Int && b is Int -> a / b
                    a is Long && b is Long -> a / b
                    a is Number && b is Number -> a.toFloat() / b.toFloat()
                    else -> PrimUnsupported
                }
            }
            else -> PrimUnsupported
        }
    }

    c["core.mod"] = { values ->
        when (values.size) {
            2 -> {
                val a = values[0]
                val b = values[1]
                when {
                    a is Int && b is Int -> a % b
                    a is Long && b is Long -> a % b
                    a is Number && b is Number -> a.toFloat() % b.toFloat()
                    else -> PrimUnsupported
                }
            }
            else -> PrimUnsupported
        }
    }

    c["core.pow"] = { values ->
        when (values.size) {
            2 -> {
                val a = values[0]
                val b = values[1]
                when {
                    a is Int && b is Int -> Math.pow(a.toDouble(), b.toDouble()).toLong()
                    a is Long && b is Long -> Math.pow(a.toDouble(), b.toDouble()).toLong()
                    a is Number && b is Number -> Math.pow(a.toDouble(), b.toDouble()).toFloat()
                    else -> PrimUnsupported
                }
            }
            else -> PrimUnsupported
        }
    }

    c["core.eq"] = { values ->
        when (values.size) {
            2 -> values[0] == values[1]
            else -> PrimUnsupported
        }
    }

    c["core.not-eq"] = { values ->
        when (values.size) {
            2 -> {
                values[0] != values[1]
            }
            else -> PrimUnsupported
        }
    }

    c["core.gt"] = { values ->
        when (values.size) {
            2 -> {
                val a = values[0]
                val b = values[1]
                when {
                    a is Number && b is Number -> Numbers.gt(a, b)
                    else -> PrimUnsupported
                }
            }
            else -> PrimUnsupported
        }
    }

    c["core.gte"] = { values ->
        when (values.size) {
            2 -> {
                val a = values[0]
                val b = values[1]
                when {
                    a is Number && b is Number -> Numbers.gte(a, b)
                    else -> PrimUnsupported
                }
            }
            else -> PrimUnsupported
        }
    }

    c["core.lt"] = { values ->
        when (values.size) {
            2 -> {
                val a = values[0]
                val b = values[1]
                when {
                    a is Number && b is Number -> Numbers.lt(a, b)
                    else -> PrimUnsupported
                }
            }
            else -> PrimUnsupported
        }
    }

    c["core.lte"] = { values ->
        when (values.size) {
            2 -> {
                val a = values[0]
                val b = values[1]
                when {
                    a is Number && b is Number -> Numbers.lte(a, b)
                    else -> PrimUnsupported
                }
            }
            else -> PrimUnsupported
        }
    }

    c["core.and"] = { values ->
        when (values.size) {
            2 -> {
                val a = values[0]
                val b = values[1]
                when {
                    a is Boolean && b is Boolean -> a && b
                    else -> PrimUnsupported
                }
            }
            else -> PrimUnsupported
        }
    }

    c["core.or"] = { values ->
        when (values.size) {
            2 -> {
                val a = values[0]
                val b = values[1]
                when {
                    a is Boolean && b is Boolean -> a || b
                    else -> PrimUnsupported
                }
            }
            else -> PrimUnsupported
        }
    }

    c["core.not"] = { values ->
        when (values.size) {
            1 -> {
                val a = values[0]
                when (a) {
                    is Boolean -> !a
                    else -> PrimUnsupported
                }
            }
            else -> PrimUnsupported
        }
    }

    c["core.floor"] = { values ->
        when (values.size) {
            1 -> {
                val a = values[0]
                when (a) {
                    is Number -> Math.floor(a.toDouble()).toInt()
                    else -> PrimUnsupported
                }
            }
            else -> PrimUnsupported
        }
    }

    c["core.ceil"] = { values ->
        when (values.size) {
            1 -> {
                val a = values[0]
                when (a) {
                    is Number -> Math.ceil(a.toDouble()).toInt()
                    else -> PrimUnsupported
                }
            }
            else -> PrimUnsupported
        }
    }

    c["core.sqrt"] = { values ->
        when (values.size) {
            1 -> {
                val a = values[0]
                when (a) {
                    is Number -> Math.sqrt(a.toDouble()).toFloat()
                    else -> PrimUnsupported
                }
            }
            else -> PrimUnsupported
        }
    }

    c["core.cons"] = { values ->
        when (values.size) {
            2 -> {
                val v = values[0]
                val l = values[1]
                when (l) {
                    is List<*> -> {
                        listOf(v, *l.toTypedArray())
                    }
                    else -> PrimUnsupported
                }
            }
            else -> PrimUnsupported
        }
    }

    c["core.field-access"] = { values ->
        when (values.size) {
            2 -> {
                val r = values[0]
                val f = values[1]
                when {
                    r is Map<*, *> && f is String -> {
                        if (r.containsKey(f)) {
                            r.get(f)
                        } else {
                            PrimHalt
                        }
                    }
                    else -> PrimUnsupported
                }
            }
            else -> PrimUnsupported
        }
    }

    c["core.make-tuple"] = { values ->
        Tuple(values)
    }

    c["core.make-list"] = { values ->
        values
    }

    c["core.make-record"] = f@{ values ->
        val m = HashMap<String, Any?>(values.size / 2)
        for (i in 0..(values.size / 2 - 1)) {
            val f = values[i * 2]
            if (f == null) {
                return@f PrimUnsupported
            } else {
                m.set(f as String, values[i * 2 + 1])
            }
        }
        m
    }

    c["core.arity-check"] = { values ->
        when (values.size) {
            2 -> {
                val r = values[0]
                val f = values[1]
                when {
                    r is Tuple && f is Number -> {
                        if (Numbers.equal(r.v.size, f)) {
                            ConstSignal
                        } else {
                            PrimHalt
                        }
                    }
                    else -> PrimUnsupported
                }
            }
            else -> PrimUnsupported
        }
    }

    c["core.list-check-size"] = { values ->
        when (values.size) {
            2 -> {
                val r = values[0]
                val f = values[1]
                when {
                    r is List<*> && f is Number -> {
                        if (Numbers.equal(r.size, f)) {
                            ConstSignal
                        } else {
                            PrimHalt
                        }
                    }
                    else -> PrimUnsupported
                }
            }
            else -> PrimUnsupported
        }
    }

    c["core.first"] = { values ->
        when (values.size) {
            1 -> {
                val l = values[0]
                when (l) {
                    is List<*> -> {
                        if (l.size > 0) {
                            l.first()
                        } else {
                            PrimHalt
                        }
                    }
                    else -> PrimUnsupported
                }
            }
            else -> PrimUnsupported
        }
    }

    c["core.rest"] = { values ->
        when (values.size) {
            1 -> {
                val l = values[0]
                when (l) {
                    is List<*> -> {
                        if (l.size > 0) {
                            l.subList(1, l.size)
                        } else {
                            PrimHalt
                        }
                    }
                    else -> PrimUnsupported
                }
            }
            else -> PrimUnsupported
        }
    }

    c["core.wrap-some"] = { values ->
        when (values.size) {
            1 -> Tuple(listOf(values[0]))
            else -> PrimUnsupported
        }
    }

    c["core.unwrap-some"] = { values ->
        when (values.size) {
            1 -> {
                val v = values[0]
                when {
                    v is Tuple && v.v.size == 1 -> v.v.first()
                    else -> PrimHalt
                }
            }
            else -> PrimUnsupported
        }
    }

    c["core.get-none"] = { values ->
        when (values.size) {
            0 -> Tuple(listOf())
            else -> PrimUnsupported
        }
    }

    c["core.is-none"] = { values ->
        when (values.size) {
            1 -> {
                val v = values[0]
                when {
                    v is Tuple && v.v.isEmpty() -> ConstSignal
                    else -> PrimHalt
                }
            }
            else -> PrimUnsupported
        }
    }

    c["core.error"] = { values ->
        when (values.size) {
            1 -> {
                System.err.println("Error: ${values[0]}")
                PrimHalt
            }
            else -> PrimUnsupported
        }
    }

    c["core.make-ref"] = { values ->
        when (values.size) {
            1 -> Ref(values[0])
            else -> PrimUnsupported
        }
    }

    c["core.deref"] = { values ->
        when (values.size) {
            1 -> {
                val ref = values[0]
                when {
                    ref is Ref -> ref.v
                    else -> PrimUnsupported
                }
            }
            else -> PrimUnsupported
        }
    }

    c["core.set"] = { values ->
        when (values.size) {
            2 -> {
                val ref = values[0]
                when {
                    ref is Ref -> {
                        ref.v = values[1]
                        ConstSignal
                    }
                    else -> PrimUnsupported
                }
            }
            else -> PrimUnsupported
        }
    }
    c["web.json_parse"] = { values ->
        when (values.size) {
            1 -> {
                val a = values[0]
                when {
                    a is String -> try {
                        val x = Parser().parse(StringBuilder(a))
                        fun toV(v: Any?): Any? {
                            return when (v) {
                                null -> null
                                is JsonArray<*> -> v.toList().map { toV(it) }
                                is Map<*, *> -> HashMap(v.mapValues { toV(it.value) })
                                is Int -> v.toLong()
                                is Number -> v
                                is String -> v
                                is Boolean -> v
                                else -> PrimUnsupported
                            }
                        }
                        toV(x)
                    } catch (e: Exception) {
                        PrimHalt
                    }
                    else -> PrimUnsupported
                }
            }
            else -> PrimUnsupported
        }
    }
    c["web.json_generate"] = { values ->
        when (values.size) {
            1 -> {
                val a = values[0]
                fun fromV(v: Any?): Any? {
                    return when (v) {
                        is Map<*, *> -> HashMap(v.mapValues { fromV(it.value) })
                        is ArrayList<*> -> v.toList().map { fromV(it) }
                        is Number -> v
                        is String -> v
                        is Boolean -> v
                        null -> null
                        else -> PrimUnsupported
                    }
                }
                @Suppress("UNCHECKED_CAST")
                JsonObject(fromV(a) as MutableMap<String, Any?>).toJsonString()
            }
            else -> PrimUnsupported
        }
    }
}
