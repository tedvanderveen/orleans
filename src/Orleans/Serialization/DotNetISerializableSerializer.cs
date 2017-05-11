using System;
using System.Reflection;
using System.Runtime.Serialization;

namespace Orleans.Serialization
{
    /// <summary>
    /// Utilities for copying, serializing, and deserializing instances of an <see cref="ISerializable"/> implementation.
    /// </summary>
    public static class DotNetISerializableSerializer
    {
        private static readonly Type SerializableType = typeof(ISerializable);
        private static readonly Type[] SerializationConstructorTypes = { typeof(SerializationInfo), typeof(StreamingContext) };
        private static readonly IFormatterConverter FormatterConverter = new FormatterConverter();

        /// <summary>
        /// Returns <see langword="true"/> if the specified type is serializable using this class, otherwise <see langword="false"/>.
        /// </summary>
        /// <returns></returns>
        public static bool IsSupportedType(Type type) => SerializableType.IsAssignableFrom(type) && GetSerializationConstructor(type) != null;

        /// <summary>
        /// Copies an <see cref="ISerializable"/> object of the specified type.
        /// </summary>
        /// <param name="source"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public static object DeepCopy(object source, ICopyContext context)
        {
            var type = source.GetType();
            var data = GetObjectData(type, (ISerializable)source, context);
            return CreateFromObjectData(type, data, context);
        }

        /// <summary>
        /// Serializes an <see cref="ISerializable"/> object of the specified type.
        /// </summary>
        /// <param name="source"></param>
        /// <param name="context"></param>
        public static void Serialize(object source, ISerializationContext context)
        {
            var data = GetObjectData(source.GetType(), (ISerializable)source, context);
            SerializeInfo(data, context);
        }

        /// <summary>
        /// Deserializes an <see cref="ISerializable"/> object of the specified type.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public static object Deserialize(Type type, IDeserializationContext context)
        {
            var data = DeserializeInfo(context);
            return CreateFromObjectData(type, data, context);
        }

        private static SerializationInfo GetObjectData(Type type, ISerializable instance, ISerializerContext context)
        {
            var serializationInfo = new SerializationInfo(type, FormatterConverter);
            instance.GetObjectData(serializationInfo, new StreamingContext(StreamingContextStates.All, context));
            return serializationInfo;
        }

        private static object CreateFromObjectData(Type type, SerializationInfo info, ISerializerContext context)
        {
            var constructor = GetSerializationConstructor(type);
            return constructor.Invoke(new object[] { info, new StreamingContext(StreamingContextStates.All, context) });
        }

        private static ConstructorInfo GetSerializationConstructor(Type itemType)
        {
            return itemType.GetConstructor(
                BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance,
                null,
                SerializationConstructorTypes,
                null);
        }

        private static void SerializeInfo(SerializationInfo info, ISerializationContext context)
        {
            var type = info.ObjectType;
            SerializationManager.SerializeInner(type, context, typeof(Type));
            SerializationManager.SerializeInner(info.IsAssemblyNameSetExplicit, context, typeof(bool));
            if (info.IsAssemblyNameSetExplicit)
            {
                SerializationManager.SerializeInner(info.AssemblyName, context, typeof(string));
            }

            SerializationManager.SerializeInner(info.IsFullTypeNameSetExplicit, context, typeof(bool));
            if (info.IsFullTypeNameSetExplicit)
            {
                SerializationManager.SerializeInner(info.FullTypeName, context, typeof(string));
            }

            SerializationManager.SerializeInner(info.MemberCount, context, typeof(int));
            foreach (var member in info)
            {
                SerializationManager.SerializeInner(member.Name, context, typeof(string));
                SerializationManager.SerializeInner(member.ObjectType, context, typeof(Type));
                SerializationManager.SerializeInner(member.Value, context, typeof(object));
            }
        }

        private static SerializationInfo DeserializeInfo(IDeserializationContext context)
        {
            var result = new SerializationInfo(typeof(int), FormatterConverter);
            context.RecordObject(result);
            var type = SerializationManager.DeserializeInner<Type>(context);
            result.SetType(type);
            var assemblyNameSet = SerializationManager.DeserializeInner<bool>(context);
            if (assemblyNameSet)
            {
                result.AssemblyName = SerializationManager.DeserializeInner<string>(context);
            }

            var fullTypeNameSet = SerializationManager.DeserializeInner<bool>(context);
            if (fullTypeNameSet)
            {
                result.FullTypeName = SerializationManager.DeserializeInner<string>(context);
            }

            var memberCount = SerializationManager.DeserializeInner<int>(context);
            while (memberCount > 0)
            {
                var memberName = SerializationManager.DeserializeInner<string>(context);
                var memberType = SerializationManager.DeserializeInner<Type>(context);
                var memberValue = SerializationManager.DeserializeInner<object>(context);
                result.AddValue(memberName, memberValue, memberType);

                memberCount--;
            }

            return result;
        }
    }
}
