package io.xorc.oam

import org.msgpack.core.MessageInsufficientBufferException
import org.msgpack.core.MessagePack
import java.io.BufferedInputStream
import java.io.ByteArrayInputStream
import java.io.File
import java.io.FileInputStream
import java.nio.file.FileSystems
import java.nio.file.Files
import kotlin.system.exitProcess
import kotlin.system.measureTimeMillis

fun printSnapshot(s: Snapshot) {
    s.values.forEach { System.err.println(" [INFO] Value: ${it}") }
    s.coeffects.forEach { System.err.println("Coeffect: ${it.id} -> ${it.description}") }
    // System.out.write(Serializer(s.state).serialize())
}

// TODO
fun parseConst(s: String): Any? {
    return Integer.parseInt(s)
}

fun main(args: Array<String>) {

    primsLoad()
    when (args[0]) {
        "byterun" -> {
            val bc = if (args.size == 1) BufferedInputStream(System.`in`) else FileInputStream(File(args[1]))
            val code = BCDeserializer(bc).deserialize()
            printSnapshot(Inter(code).run())
        }

        "unblock" -> {
            val code = BCDeserializer(Files.newInputStream(FileSystems.getDefault().getPath(args[1]))).deserialize()
            val coeffect = Integer.parseInt(args[2])
            val value = parseConst(args[3])
            val instance = Deserializer(BufferedInputStream(System.`in`)).deserialize()
            printSnapshot(Inter(code).unblock(instance, coeffect, value))
        }

        "repl" -> {
            val input = BufferedInputStream(System.`in`)
            val decoder = MessagePack.newDefaultUnpacker(input)
            var code: BC? = null
            var state: ByteArray? = null
            while (true) {
                try {
                    decoder.unpackArrayHeader()
                    when (decoder.unpackInt()) {
                        0 -> {
                            code = BCDeserializer(decoder).deserialize()
                            val inter = Inter(code)
                            val snapshot = inter.run()
                            val result = Result(snapshot.values, snapshot.coeffects, snapshot.killedCoeffects)
                            if (snapshot.state.isRunning()) {
                                state = Serializer(snapshot.state).serialize()
                            } else {
                                state = null
                            }
                            System.out.write(ResultSerializer(result).serialize())
                        }
                        1 -> {
                            val id = decoder.unpackInt()
                            val (_, v) = deserializeSimpleValue(decoder)
                            if (state == null) {
                                throw RuntimeException()
                            } else {
                                val inter = Inter(code!!)
                                val instance = Deserializer(ByteArrayInputStream(state)).deserialize()
                                val snapshot = inter.unblock(instance, id, v)
                                val result = Result(snapshot.values, snapshot.coeffects, snapshot.killedCoeffects)
                                if (snapshot.state.isRunning()) {
                                    state = Serializer(snapshot.state).serialize()
                                } else {
                                    state = null
                                }
                                System.out.write(ResultSerializer(result).serialize())
                            }
                        }
                        2 -> {
                            code = BCDeserializer(decoder).deserialize()
                            val n = decoder.unpackInt()
                            val inter = Inter(code)
                            for (i in 1..n) {
                                inter.run()
                            }
                            val res = measureTimeMillis {
                                for (i in 1..n) {
                                    inter.run()
                                }
                            }
                            val encoder = MessagePack.newDefaultBufferPacker()
                            encoder.packFloat(res.toFloat())
                            System.out.write(encoder.toByteArray())
                        }
                    }
                } catch (e: MessageInsufficientBufferException) {
                    exitProcess(0)
                }
            }
        }
    }

}
