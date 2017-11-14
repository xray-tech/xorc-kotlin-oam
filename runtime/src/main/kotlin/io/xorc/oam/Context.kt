package io.xorc.oam

class UnknownFFC(val def: String) : RuntimeException("Unknown FFC: $def")

val pseudoFFC = listOf("core.make-pending", "core.pending-read",
        "core.realize", "core.is-realized",
        "core.stop-pending")
class Context {
    private object Global {
        val INSTANCE = Context()
        init {
            for (pseduo in pseudoFFC) {
                INSTANCE[pseduo] = { _ -> PrimUnsupported }
            }
        }
    }
    companion object {
        @JvmStatic
        fun getInstance() = Global.INSTANCE
    }

    val repo = mutableListOf<Prim>()
    val mapping = mutableMapOf<String, Int>()

    inner class Snapshot(val mapping: List<Pair<String, Int>>) {
        operator fun get(index: Int): Prim {
            return repo[mapping[index].second]
        }

        fun getName(index: Int): String {
            return mapping[index].first
        }
    }

    fun snapshot(ffc: List<String>): Snapshot {
        val snapshotMapping = MutableList(ffc.size) { i ->
            val repoIndex = mapping.get(ffc[i]) ?: throw UnknownFFC(ffc[i])
            Pair(ffc[i], repoIndex)
        }
        return Snapshot(snapshotMapping)
    }

    operator fun set(def: String, prim: (List<Any?>) -> Any?) {
        mapping.set(def, repo.size)
        repo.add(prim)
    }
}



