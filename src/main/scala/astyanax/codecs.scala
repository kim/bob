package astyanax

trait Codecs {

    import java.nio.ByteBuffer


    trait Codec[A] {
        def encode(a: A): ByteBuffer
        def decode(b: ByteBuffer): A
    }

    object BytesCodec extends Codec[ByteBuffer] {
        def encode(b: ByteBuffer): ByteBuffer = b
        def decode(b: ByteBuffer): ByteBuffer = b
    }

    object Utf8Codec extends Codec[String] {
        def encode(s: String): ByteBuffer =
            ByteBuffer.wrap(s.getBytes("UTF-8"))

        def decode(b: ByteBuffer): String =
            new String(bb2ba(b), b.position, b.remaining, "UTF-8")

        private[this]
        def bb2ba(bb: ByteBuffer): Array[Byte] =
            if (bb.hasArray) bb.array
            else {
                val ba = new Array[Byte](bb.remaining)
                bb.get(ba)
                ba
            }
    }

    object LongCodec extends Codec[Long] {
        def encode(l: Long): ByteBuffer = {
            val buf = ByteBuffer.allocate(8)
            buf.putLong(l)
            buf.rewind
            buf
        }

        def decode(b: ByteBuffer): Long = {
            require(b.remaining >= 8)
            b.duplicate.getLong
        }
    }

    object IntCodec extends Codec[Int] {
        def encode(i: Int): ByteBuffer = {
            val buf = ByteBuffer.allocate(4)
            buf.putInt(i)
            buf.rewind
            buf
        }

        def decode(b: ByteBuffer): Int = {
            require(b.remaining >= 4)
            b.duplicate.getInt
        }
    }
}

object Codecs extends Codecs


// vim: set ts=4 sw=4 et:
