using System;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.Serialization;
using Orleans.Utilities;

namespace Orleans.Serialization
{
    internal sealed class DotNetSerializableSerializer : IKeyedSerializer
    {
        private readonly IFormatterConverter formatterConverter = new FormatterConverter();
        private readonly TypeInfo serializableType = typeof(ISerializable).GetTypeInfo();
        private readonly SerializationConstructorFactory constructorFactory = new SerializationConstructorFactory();
        private readonly SerializationCallbacksFactory serializationCallbacks = new SerializationCallbacksFactory();
        private readonly ValueTypeSerializerFactory valueTypeSerializerFactory;
        private readonly ISerializer objectSerializer;

        public DotNetSerializableSerializer()
        {
            this.objectSerializer = new ObjectSerializer(this.constructorFactory, this.serializationCallbacks, this.formatterConverter);
            this.valueTypeSerializerFactory = new ValueTypeSerializerFactory(this.constructorFactory, this.serializationCallbacks, this.formatterConverter);
        }

        /// <inheritdoc />
        public byte SerializerId => 1;
        
        /// <inheritdoc />
        public bool IsSupportedType(Type itemType) => this.serializableType.IsAssignableFrom(itemType) && itemType.IsSerializable &&
                                                      this.constructorFactory.GetSerializationConstructor(itemType) != null;

        /// <inheritdoc />
        public object DeepCopy(object source, ICopyContext context)
        {
            var type = source.GetType();
            if (type.IsValueType)
            {
                var serializer = this.valueTypeSerializerFactory.GetSerializer(type);
                return serializer.DeepCopy(source, context);
            }

            return this.objectSerializer.DeepCopy(source, context);
        }

        /// <inheritdoc />
        public void Serialize(object item, ISerializationContext context, Type expectedType)
        {
            var type = item.GetType();
            if (type.IsValueType)
            {
                var serializer = this.valueTypeSerializerFactory.GetSerializer(type);
                serializer.Serialize(item, context);
            }
            else
            {
                this.objectSerializer.Serialize(item, context);
            }
        }

        /// <inheritdoc />
        public object Deserialize(Type expectedType, IDeserializationContext context)
        {
            var startOffset = context.CurrentObjectOffset;
            var type = SerializationManager.DeserializeInner<Type>(context);
            if (type.IsValueType)
            {
                var serializer = this.valueTypeSerializerFactory.GetSerializer(type);
                return serializer.Deserialize(type, startOffset, context);
            }

            return this.objectSerializer.Deserialize(type, startOffset, context);
        }

        internal interface ISerializer
        {
            object Deserialize(Type type, int startOffset, IDeserializationContext context);
            void Serialize(object item, ISerializationContext context);
            object DeepCopy(object source, ICopyContext context);
        }

        internal class ObjectSerializer : ISerializer
        {
            private readonly IFormatterConverter formatterConverter;
            private readonly SerializationConstructorFactory constructorFactory;
            private readonly SerializationCallbacksFactory serializationCallbacks;

            public ObjectSerializer(
                SerializationConstructorFactory constructorFactory,
                SerializationCallbacksFactory serializationCallbacks,
                IFormatterConverter formatterConverter)
            {
                this.constructorFactory = constructorFactory;
                this.serializationCallbacks = serializationCallbacks;
                this.formatterConverter = formatterConverter;
            }

            /// <inheritdoc />
            public object DeepCopy(object source, ICopyContext context)
            {
                var type = source.GetType();
                var callbacks = this.serializationCallbacks.GetReferenceTypeCallbacks(type);
                var serializable = (ISerializable)source;
                var result = FormatterServices.GetUninitializedObject(type);
                context.RecordCopy(source, result);

                // Shallow-copy the object into the serialization info.
                var originalInfo = new SerializationInfo(type, this.formatterConverter);
                var streamingContext = new StreamingContext(StreamingContextStates.All, context);
                callbacks.OnSerializing?.Invoke(source, streamingContext);
                serializable.GetObjectData(originalInfo, streamingContext);

                // Deep-copy the serialization info.
                var copyInfo = new SerializationInfo(type, this.formatterConverter);
                foreach (var item in originalInfo)
                {
                    copyInfo.AddValue(item.Name, SerializationManager.DeepCopyInner(item.Value, context));
                }
                callbacks.OnSerialized?.Invoke(source, streamingContext);
                callbacks.OnDeserializing?.Invoke(result, streamingContext);

                // Shallow-copy the serialization info into the result.
                var constructor = this.constructorFactory.GetSerializationConstructorDelegate(type);
                constructor(result, copyInfo, streamingContext);
                callbacks.OnDeserialized?.Invoke(result, streamingContext);
                if (result is IDeserializationCallback callback)
                {
                    callback.OnDeserialization(context);
                }

                return result;
            }

            /// <inheritdoc />
            public void Serialize(object item, ISerializationContext context)
            {
                var type = item.GetType();
                var callbacks = this.serializationCallbacks.GetReferenceTypeCallbacks(type);
                var info = new SerializationInfo(type, this.formatterConverter);
                var streamingContext = new StreamingContext(StreamingContextStates.All, context);
                callbacks.OnSerializing?.Invoke(item, streamingContext);
                ((ISerializable)item).GetObjectData(info, streamingContext);

                SerializationManager.SerializeInner(type, context);
                SerializationManager.SerializeInner(info.MemberCount, context);
                foreach (var entry in info)
                {
                    SerializationManager.SerializeInner(entry.Name, context);
                    var fieldType = entry.Value?.GetType();
                    SerializationManager.SerializeInner(fieldType, context);
                    SerializationManager.SerializeInner(entry.Value, context, fieldType);
                }

                callbacks.OnSerialized?.Invoke(item, streamingContext);
            }

            /// <inheritdoc />
            public object Deserialize(Type type, int startOffset, IDeserializationContext context)
            {
                var callbacks = this.serializationCallbacks.GetReferenceTypeCallbacks(type);
                var result = FormatterServices.GetUninitializedObject(type);
                context.RecordObject(result, startOffset);

                var memberCount = SerializationManager.DeserializeInner<int>(context);

                var info = new SerializationInfo(type, this.formatterConverter);
                var streamingContext = new StreamingContext(StreamingContextStates.All, context);
                callbacks.OnDeserializing?.Invoke(result, streamingContext);

                for (var i = 0; i < memberCount; i++)
                {
                    var name = SerializationManager.DeserializeInner<string>(context);
                    var fieldType = SerializationManager.DeserializeInner<Type>(context);
                    var value = SerializationManager.DeserializeInner(fieldType, context);
                    info.AddValue(name, value);
                }

                var constructor = this.constructorFactory.GetSerializationConstructorDelegate(type);
                constructor(result, info, streamingContext);
                callbacks.OnDeserialized?.Invoke(result, streamingContext);
                if (result is IDeserializationCallback callback)
                {
                    callback.OnDeserialization(context);
                }

                return result;
            }
        }

        internal class ValueTypeSerializer<T> : ISerializer where T : struct
        {
            public delegate void ValueConstructor(ref T value, SerializationInfo info, StreamingContext context);
            public delegate void SerializationCallback(ref T value, StreamingContext context);

            private readonly ValueConstructor constructor;
            private readonly SerializationCallbacksFactory.SerializationCallbacks<SerializationCallback> callbacks;
            private readonly IFormatterConverter formatterConverter;

            public ValueTypeSerializer(
                ValueConstructor constructor,
                SerializationCallbacksFactory.SerializationCallbacks<SerializationCallback> callbacks,
                IFormatterConverter formatterConverter)
            {
                this.constructor = constructor;
                this.callbacks = callbacks;
                this.formatterConverter = formatterConverter;
            }

            public object Deserialize(Type type, int startOffset, IDeserializationContext context)
            {
                var result = default(T);
                var memberCount = SerializationManager.DeserializeInner<int>(context);

                var info = new SerializationInfo(type, this.formatterConverter);
                var streamingContext = new StreamingContext(StreamingContextStates.All, context);
                this.callbacks.OnDeserializing?.Invoke(ref result, streamingContext);

                for (var i = 0; i < memberCount; i++)
                {
                    var name = SerializationManager.DeserializeInner<string>(context);
                    var fieldType = SerializationManager.DeserializeInner<Type>(context);
                    var value = SerializationManager.DeserializeInner(fieldType, context);
                    info.AddValue(name, value);
                }

                this.constructor(ref result, info, streamingContext);
                this.callbacks.OnDeserialized?.Invoke(ref result, streamingContext);
                if (result is IDeserializationCallback callback)
                {
                    callback.OnDeserialization(context);
                }

                return result;
            }

            public void Serialize(object item, ISerializationContext context)
            {
                var localItem = (T) item;
                var type = item.GetType();
                var info = new SerializationInfo(type, this.formatterConverter);
                var streamingContext = new StreamingContext(StreamingContextStates.All, context);
                this.callbacks.OnSerializing?.Invoke(ref localItem, streamingContext);
                ((ISerializable)item).GetObjectData(info, streamingContext);

                SerializationManager.SerializeInner(type, context);
                SerializationManager.SerializeInner(info.MemberCount, context);
                foreach (var entry in info)
                {
                    SerializationManager.SerializeInner(entry.Name, context);
                    var fieldType = entry.Value?.GetType();
                    SerializationManager.SerializeInner(fieldType, context);
                    SerializationManager.SerializeInner(entry.Value, context, fieldType);
                }

                this.callbacks.OnSerialized?.Invoke(ref localItem, streamingContext);
            }

            public object DeepCopy(object source, ICopyContext context)
            {
                var localSource = (T) source;
                var type = source.GetType();
                var serializable = (ISerializable)source;
                var result = default(T);

                // Shallow-copy the object into the serialization info.
                var originalInfo = new SerializationInfo(type, this.formatterConverter);
                var streamingContext = new StreamingContext(StreamingContextStates.All, context);
                this.callbacks.OnSerializing?.Invoke(ref localSource, streamingContext);
                serializable.GetObjectData(originalInfo, streamingContext);

                // Deep-copy the serialization info.
                var copyInfo = new SerializationInfo(type, this.formatterConverter);
                foreach (var item in originalInfo)
                {
                    copyInfo.AddValue(item.Name, SerializationManager.DeepCopyInner(item.Value, context));
                }

                this.callbacks.OnSerialized?.Invoke(ref localSource, streamingContext);
                this.callbacks.OnDeserializing?.Invoke(ref localSource, streamingContext);

                // Shallow-copy the serialization info into the result.
                this.constructor(ref result, copyInfo, streamingContext);
                this.callbacks.OnDeserialized?.Invoke(ref result, streamingContext);
                if (result is IDeserializationCallback callback)
                {
                    callback.OnDeserialization(context);
                }

                return result;
            }
        }

        /// <summary>
        /// Creates <see cref="ISerializer"/> instances for value types.
        /// </summary>
        internal class ValueTypeSerializerFactory
        {
            private readonly SerializationConstructorFactory constructorFactory;
            private readonly SerializationCallbacksFactory callbacksFactory;
            private readonly IFormatterConverter formatterConverter;
            private readonly Func<Type, ISerializer> createSerializerDelegate;

            private readonly CachedReadConcurrentDictionary<Type, ISerializer> serializers =
                new CachedReadConcurrentDictionary<Type, ISerializer>();

            private readonly MethodInfo createTypedSerializerMethodInfo = typeof(ValueTypeSerializerFactory).GetMethod(
                nameof(CreateTypedSerializer),
                BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);

            public ValueTypeSerializerFactory(
                SerializationConstructorFactory constructorFactory,
                SerializationCallbacksFactory callbacksFactory,
                IFormatterConverter formatterConverter)
            {
                this.constructorFactory = constructorFactory;
                this.callbacksFactory = callbacksFactory;
                this.formatterConverter = formatterConverter;
                this.createSerializerDelegate = type => (ISerializer) this
                    .createTypedSerializerMethodInfo.MakeGenericMethod(type).Invoke(this, null);
            }

            public ISerializer GetSerializer(Type type)
            {
                return this.serializers.GetOrAdd(type, this.createSerializerDelegate);
            }

            private ISerializer CreateTypedSerializer<T>() where T : struct
            {
                var constructor = this.constructorFactory.GetSerializationConstructorDelegate<T, ValueTypeSerializer<T>.ValueConstructor>();
                var callbacks =
                    this.callbacksFactory.GetValueTypeCallbacks<T, ValueTypeSerializer<T>.SerializationCallback>(typeof(T));
                return new ValueTypeSerializer<T>(constructor, callbacks, this.formatterConverter);
            }
        }

        /// <summary>
        /// Creates delegates for calling ISerializable-conformant constructors.
        /// </summary>
        internal class SerializationConstructorFactory
        {
            private readonly Func<Type, object> createConstructorDelegate;

            private readonly CachedReadConcurrentDictionary<Type, object> constructors =
                new CachedReadConcurrentDictionary<Type, object>();

            private readonly Type[] serializationConstructorParameterTypes = {typeof(SerializationInfo), typeof(StreamingContext)};

            public SerializationConstructorFactory()
            {
                this.createConstructorDelegate = this
                    .GetSerializationConstructorInvoker<object, Action<object, SerializationInfo, StreamingContext>>;
            }

            public Action<object, SerializationInfo, StreamingContext> GetSerializationConstructorDelegate(Type type)
            {
                return (Action<object, SerializationInfo, StreamingContext>)this.constructors.GetOrAdd(
                    type,
                    this.createConstructorDelegate);
            }
            public TConstructor GetSerializationConstructorDelegate<TOwner, TConstructor>()
            {
                return (TConstructor) this.constructors.GetOrAdd(
                    typeof(TOwner),
                    type => (object) this.GetSerializationConstructorInvoker<TOwner, TConstructor>(type));
            }

            public ConstructorInfo GetSerializationConstructor(Type type)
            {
                return type.GetConstructor(
                    BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance,
                    null,
                    this.serializationConstructorParameterTypes,
                    null);
            }

            private TConstructor GetSerializationConstructorInvoker<TOwner, TConstructor>(Type type)
            {
                var constructor = this.GetSerializationConstructor(type);
                if (constructor == null) throw new SerializationException($"{nameof(ISerializable)} constructor not found on type {type}.");

                Type[] parameterTypes;
                if (typeof(TOwner).IsValueType) parameterTypes = new[] { typeof(TOwner).MakeByRefType(), typeof(SerializationInfo), typeof(StreamingContext) };
                else parameterTypes = new[] { typeof(object), typeof(SerializationInfo), typeof(StreamingContext) };
                
                var method = new DynamicMethod($"{type}_serialization_ctor", null, parameterTypes, typeof(TOwner), skipVisibility: true);
                var il = method.GetILGenerator();

                il.Emit(OpCodes.Ldarg_0);
                if (type != typeof(TOwner)) il.Emit(OpCodes.Castclass, type);
                il.Emit(OpCodes.Ldarg_1);
                il.Emit(OpCodes.Ldarg_2);
                il.Emit(OpCodes.Call, constructor);
                il.Emit(OpCodes.Ret);

                object result = method.CreateDelegate(typeof(TConstructor));
                return (TConstructor)result;
            }
        }

        /// <summary>
        /// Creates delegates for calling methods marked with serialization attributes.
        /// </summary>
        internal class SerializationCallbacksFactory
        {
            private readonly CachedReadConcurrentDictionary<Type, object> cache =
                new CachedReadConcurrentDictionary<Type, object>();
            private readonly Func<Type, object> factory;
            
            public SerializationCallbacksFactory()
            {
                this.factory = this.CreateTypedCallbacks<object, Action<object, StreamingContext>>;
            }

            public SerializationCallbacks<Action<object, StreamingContext>> GetReferenceTypeCallbacks(Type type) => (
                SerializationCallbacks<Action<object, StreamingContext>>)this.cache.GetOrAdd(type, this.factory);

            public SerializationCallbacks<TDelegate> GetValueTypeCallbacks<TOwner, TDelegate>(Type type) => (
                SerializationCallbacks<TDelegate>) this.cache.GetOrAdd(type, t => (object) this.CreateTypedCallbacks<TOwner, TDelegate>(type));

            private SerializationCallbacks<TDelegate> CreateTypedCallbacks<TOwner, TDelegate>(Type type)
            {
                var typeInfo = type.GetTypeInfo();
                var onDeserializing = default(TDelegate);
                var onDeserialized = default(TDelegate);
                var onSerializing = default(TDelegate);
                var onSerialized = default(TDelegate);
                foreach (var method in typeInfo.GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic))
                {
                    var parameterInfos = method.GetParameters();
                    if (parameterInfos.Length != 1) continue;
                    if (parameterInfos[0].ParameterType != typeof(StreamingContext)) continue;

                    if (method.GetCustomAttribute<OnDeserializingAttribute>() != null)
                    {
                        onDeserializing = GetSerializationMethod<TOwner, TDelegate>(typeInfo, method);
                    }

                    if (method.GetCustomAttribute<OnDeserializedAttribute>() != null)
                    {
                        onDeserialized = GetSerializationMethod<TOwner, TDelegate>(typeInfo, method);
                    }

                    if (method.GetCustomAttribute<OnSerializingAttribute>() != null)
                    {
                        onSerializing = GetSerializationMethod<TOwner, TDelegate>(typeInfo, method);
                    }

                    if (method.GetCustomAttribute<OnSerializedAttribute>() != null)
                    {
                        onSerialized = GetSerializationMethod<TOwner, TDelegate>(typeInfo, method);
                    }
                }

                return new SerializationCallbacks<TDelegate>(onDeserializing, onDeserialized, onSerializing, onSerialized);
            }

            private static TDelegate GetSerializationMethod<TOwner, TDelegate>(Type type, MethodInfo callbackMethod)
            {
                Type[] callbackParameterTypes;
                if (typeof(TOwner).IsValueType) callbackParameterTypes = new[] { typeof(TOwner).MakeByRefType(), typeof(StreamingContext) };
                else callbackParameterTypes = new[] { typeof(object), typeof(StreamingContext) };

                var method = new DynamicMethod($"{callbackMethod.Name}_Trampoline", null, callbackParameterTypes, type, skipVisibility: true);
                var il = method.GetILGenerator();

                il.Emit(OpCodes.Ldarg_0);
                if (type != typeof(TOwner)) il.Emit(OpCodes.Castclass, type);
                il.Emit(OpCodes.Ldarg_1);
                il.Emit(OpCodes.Callvirt, callbackMethod);
                il.Emit(OpCodes.Ret);

                object result = method.CreateDelegate(typeof(TDelegate));
                return (TDelegate)result;
            }

            public class SerializationCallbacks<TDelegate>
            {
                public SerializationCallbacks(
                    TDelegate onDeserializing,
                    TDelegate onDeserialized,
                    TDelegate onSerializing,
                    TDelegate onSerialized)
                {
                    this.OnDeserializing = onDeserializing;
                    this.OnDeserialized = onDeserialized;
                    this.OnSerializing = onSerializing;
                    this.OnSerialized = onSerialized;
                }

                public TDelegate OnDeserializing { get; }
                public TDelegate OnDeserialized { get; }
                public TDelegate OnSerializing { get; }
                public TDelegate OnSerialized { get; }
            }
        }
    }
}
