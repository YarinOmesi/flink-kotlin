package com.lapanthere.flink.api.kotlin.typeutils

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.serialization.SerializerConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.runtime.NullableSerializer

/**
 * Type information to handle nullable fields.
 * Wraps [innerTypeInformation] serializer with [NullableSerializer].
 */
public class NullableTypeInfo<T>(
    public val innerTypeInformation: TypeInformation<T>,
    private val padNullValueIfFixedLen: Boolean = false
) : TypeInformation<T>() {
    override fun isBasicType(): Boolean = innerTypeInformation.isBasicType

    override fun isTupleType(): Boolean = innerTypeInformation.isTupleType

    override fun getArity(): Int = innerTypeInformation.arity

    override fun getTotalFields(): Int = innerTypeInformation.totalFields

    override fun getTypeClass(): Class<T?>? = innerTypeInformation.typeClass

    override fun isKeyType(): Boolean = innerTypeInformation.isKeyType

    @Deprecated("Deprecated in Java")
    override fun createSerializer(config: ExecutionConfig?): TypeSerializer<T?>? = createSerializer(config?.serializerConfig)

    override fun createSerializer(config: SerializerConfig?): TypeSerializer<T?>? {
        val serializer = innerTypeInformation.createSerializer(config)
        return NullableSerializer.wrap<T>(serializer, padNullValueIfFixedLen)
    }

    override fun getGenericParameters(): Map<String?, TypeInformation<*>?>? = innerTypeInformation.genericParameters

    override fun isSortKeyType(): Boolean = innerTypeInformation.isSortKeyType

    override fun toString(): String {
        return "NullableTypeInfo<$innerTypeInformation>"
    }

    override fun equals(other: Any?): Boolean {
        if(other !is NullableTypeInfo<*>) return false
        return other.canEqual(this) && innerTypeInformation.equals(other)
    }

    override fun hashCode(): Int = innerTypeInformation.hashCode()

    override fun canEqual(obj: Any?): Boolean = obj is NullableTypeInfo<*> && obj.innerTypeInformation.canEqual(this)
}
