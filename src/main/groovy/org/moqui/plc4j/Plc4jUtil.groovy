/*
 * This software is in the public domain under CC0 1.0 Universal plus a
 * Grant of Patent License.
 *
 * To the extent possible under law, the author(s) have dedicated all
 * copyright and related and neighboring rights to this software to the
 * public domain worldwide. This software is distributed without any
 * warranty.
 *
 * You should have received a copy of the CC0 Public Domain Dedication
 * along with this software (see the LICENSE.md file). If not, see
 * <http://creativecommons.org/publicdomain/zero/1.0/>.
 */
package org.moqui.plc4j

import java.math.RoundingMode
import org.moqui.entity.EntityValue
import org.apache.plc4x.java.api.value.PlcValue
import org.slf4j.Logger
import org.slf4j.LoggerFactory

final class Plc4jUtil {

    private Plc4jUtil() { }

    private static final Logger logger = LoggerFactory.getLogger(Plc4jUtil.class)

    /**
     * Build the connectionString from the DeviceConnection entity value.
     * Plc4j url format: {driver code}:{transport code}://{transport config}?{options} - i.e. modbus-tcp:tcp://127.0.0.1:502, simulated://127.0.0.1
     */
    static String buildConnectionString(final EntityValue connection) {
        if (!connection) return null
        return connection.driver.enumCode + (connection.transport? ":" + connection.transport.enumCode : "") +
            (connection.transportConfig ? "://" + connection.transportConfig : "") + (connection.options ? "?" + connection.options : "")
    }

    /** Map a physical PlcValue -> logical Parameter */
    static EntityValue processInputValue(final EntityValue item, final PlcValue physicalValue) {
        final EntityValue parameterDef = item.parameter.parameterDef
        EntityValue parameter = item.parameter.cloneValue()
 
        switch (parameterDef.parameterTypeEnumId) {
            case 'PtTextIndicator':
                // xor for reverseLogic
                parameter.symbolicValue = ((physicalValue.getBoolean() ^ (item.reverseLogic == 'Y')) ? 'Y' : 'N')
                return parameter

            case 'PtByte':
            case 'PtBitSet':
            case 'PtEnumeration':
                int width = item.itemTypeEnumId == 'DritByte' ? 8 : item.itemTypeEnumId == 'DritDWord' ? 32 : 16
                String bits = normalizeBits(physicalValue.getString(), width)
                parameter.parameterEnumId = bits
                return parameter

            default:
                if ('PtNumber' == parameterDef.type.parentEnumId) {
                    // Value processing
                    BigDecimal numericValue = physicalValue.getBigDecimal()
                    BigDecimal lastNumericalValue = parameter.numericValue ?: BigDecimal.ZERO

                    // optional physical -> logical mapping
                    numericValue = mapping(numericValue, item.minItemValue, item.maxItemValue,
                        parameterDef.minValue, parameterDef.maxValue)

                    // logical scale/offset
                    if (item.scalingFactor.compareTo(BigDecimal.ZERO) == 0) item.scalingFactor = BigDecimal.ONE
                    numericValue = numericValue.multiply(item.scalingFactor).add(item.offsetValue)

                    // clamp + digits
                    if (parameterDef.maxValue && parameterDef.minValue)
                        numericValue = numericValue.max(parameterDef.minValue).min(parameterDef.maxValue)
                    if (item.significantDigits)
                        numericValue = numericValue.setScale(item.significantDigits as int, RoundingMode.HALF_UP)

                    // update decision, log are always processed
                    boolean changedEnough = (numericValue.compareTo(lastNumericalValue.minus(item.tolerance)) < 0)
                        || (numericValue.compareTo(lastNumericalValue.plus(item.tolerance)) > 0)
                    boolean shouldUpdate = ((item.allowDuplicate == 'Y') || changedEnough || item.request.purposeEnumId == 'DrpLogging')
                    if (shouldUpdate) {
                        parameter.numericValue = numericValue
                        return parameter
                    }

                    return null
                }

                // all other types -> string
                parameter.symbolicValue = physicalValue.getString()
                return parameter
        }
    }

    /** Logical Parameter -> physical value to write. */
    static Object processOutputValue(final EntityValue item) {
        final EntityValue parameter = item.parameter
        final EntityValue parameterDef = parameter.parameterDef

        switch (parameterDef.parameterTypeEnumId) {
            case 'PtTextIndicator':
                // xor for reverseLogic
                return (parameter.symbolicValue == 'Y') ^ (item.reverseLogic == 'Y') as boolean

            case 'PtEnumeration':
            case 'PtByte':
            case 'PtBitSet':
                int width = item.itemTypeEnumId == 'DritByte' ? 8 : item.itemTypeEnumId == 'DritDWord' ? 32 : 16
                String bits = normalizeBits(parameter.parameterEnumId as String, width)
                if (!bits) return null
                BitSet bs = bitStringToBitSet(bits)

                switch (item.itemTypeEnumId) {
                    case 'DritByte':
                        return bitSetToUInt(bs, 8)
                    case 'DritWord':
                        return bitSetToUInt(bs, 16) as int
                    case 'DritDWord':
                        return bitSetToUInt(bs, 32) as long
                }
                return null

            default:
                if (parameterDef.type?.parentEnumId == 'PtNumber') {
                    BigDecimal numericValue = parameter.numericValue

                    // reverse logical scale and offset
                    if (item.scalingFactor.compareTo(BigDecimal.ZERO) == 0) item.scalingFactor = BigDecimal.ONE
                    numericValue = numericValue.subtract(item.offsetValue).divide(item.scalingFactor, RoundingMode.HALF_UP)

                    // clamp in logical domain
                    if (parameterDef.maxValue && parameterDef.minValue) {
                        numericValue = numericValue.max(parameterDef.minValue).min(parameterDef.maxValue)
                    }

                    // logical -> physical mapping
                    numericValue = mapping(numericValue, parameterDef.minValue, parameterDef.maxValue,
                        item.minItemValue, item.maxItemValue)

                    // digits
                    if (item.significantDigits) {
                        numericValue = numericValue.setScale(item.significantDigits as int, RoundingMode.HALF_UP)
                    }

                    // integer case
                    if (parameterDef.parameterTypeEnumId == 'PtNumberInteger') return numericValue.intValue()
                    return numericValue
                }

                // all other types -> string
                return parameter.symbolicValue
        }
    }

    // Bulk create or update — skips nulls (MAJ-5), logs errors without blocking (BLK-2)
    static void createOrUpdateBulk(List<EntityValue> valueList) {
        if (!valueList) return
        for (EntityValue ev : valueList) {
            if (!ev) continue
            try {
                ev.createOrUpdate()
            } catch (Throwable t) {
                logger.error("Failed to persist entity value: ${t.class.simpleName}: ${t.message}", t)
            }
        }
    }

    //  BitSet operations

    /** Left -> Right: index 0 is leftmost char. */
    static BitSet bitsetLR(String bits) {
        BitSet bs = new BitSet(bits.length())
        for (int i=0;i<bits.length();i++) if (bits.charAt(i) == '1') bs.set(i)
        return bs
    }
    /** Right -> Left: index 0 is rightmost char (LSB). */
    static BitSet bitsetRL(String bits) {
        int len = bits.length()
        BitSet bs = new BitSet(len)
        for (int i=len-1;i>=0;i--) if (bits.charAt(i) == '1') bs.set(len - i - 1)
        return bs
    }

    // Parse "1000010001101111" -> BitSet (bit0 = LSB)
    static BitSet bitStringToBitSet(String bits) {
        String s = bits.replaceAll(/[ _]/, '')
        if (s.startsWith('0b') || s.startsWith('0B')) s = s.substring(2)
        BitSet bs = new BitSet(s.length())
        // BitSet index 0 is LSB. Our strings are MSB->LSB, so flip index.
        int n = s.length()
        for (int i = 0; i < n; i++) {
            if (s.charAt(n - 1 - i) == '1') bs.set(i)
        }
        return bs
    }

    static long bitSetToUInt(BitSet bs, int width) {
        long value = 0L
        for (int i = bs.nextSetBit(0); i >= 0 && i < width; i = bs.nextSetBit(i + 1)) value |= (1L << i)
        long mask = (width >= 64) ? -1L : ((1L << width) - 1L)
        return (value & mask)
    }

    // BitSet -> byte[] (msbFirst controls bit packing within each byte)
    static byte[] bitSetToBytes(BitSet bs, int byteCount, boolean msbFirstPerByte = true) {
        byte[] out = new byte[byteCount]
        for (int bit = 0; bit < byteCount * 8; bit++) {
            if (bs.get(bit)) {
                int byteIx = bit / 8
                int bitIx  = bit % 8
                int pos = msbFirstPerByte ? (7 - bitIx) : bitIx
                out[byteIx] = (byte)(out[byteIx] | (1 << pos))
            }
        }
        return out
    }

    static String toBitsFixedWidth(long value, int width) {
        long mask = (width == 64) ? -1L : ((1L << width) - 1L)
        String bits = Long.toBinaryString(value & mask)
        return bits.padLeft(width, '0')
    }

    static String normalizeBits(String raw, int width) {
        if (!raw) return null
        String r = raw.trim()
        if (r.startsWith('0b') || r.startsWith('0B')) r = r.substring(2)
        r = r.replaceAll(/[ _]/, '')
        if (r ==~ /^[01]+$/) return r.padLeft(width, '0')
        if (r ==~ /^[+-]?\d+$/) return toBitsFixedWidth(Long.parseLong(r), width)
        // fallback: strip non-bits and pad
        return r.replaceAll(/[^01]/, '').padLeft(width, '0')
    }

    // Helpers

    static BigDecimal mapping(BigDecimal v, BigDecimal inMin, BigDecimal inMax, BigDecimal outMin, BigDecimal outMax) {
        if (!inMax || !inMin || !outMax || !outMin) return v
        if (inMax.compareTo(inMin) <= 0) return outMin
        return (v.subtract(inMin))
            .divide(inMax.subtract(inMin), 12, RoundingMode.HALF_UP)
            .multiply(outMax.subtract(outMin))
            .add(outMin)
    }
}
