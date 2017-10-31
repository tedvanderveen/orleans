using System;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using Orleans.ServiceFabric.Actors.Runtime;

namespace Orleans.ServiceFabric.Actors
{
    internal static class ReleaseAssert
    {
        [SuppressMessage("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode", Justification = "This source file is included in building more than one assembly. The function is not called in one of the assembly, but used in the other ones.")]
        public static void AssertIfNot(bool cond, string format, params object[] args)
        {
            if (cond)
                return;
            Failfast(format, args);
        }

        [SuppressMessage("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode", Justification = "This source file is included in building more than one assembly. The function is not called in one of the assembly, but used in the other ones.")]
        public static void AssertIf(bool cond, string format, params object[] args)
        {
            if (!cond)
                return;
            Failfast(format, args);
        }

        [SuppressMessage("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode", Justification = "This source file is included in building more than one assembly. The function is not called in one of the assembly, but used in the other ones.")]
        public static void AssertIfNull(object argument, string argumentName)
        {
            if (argument != null)
                return;
            Failfast($"Argument \"{argumentName}\" null.");
        }

        [SuppressMessage("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode", Justification = "This source file is included in building more than one assembly. The function is not called in one of the assembly, but used in the other ones.")]
        public static void Failfast(string format, params object[] args)
        {
            string message = string.Format(CultureInfo.InvariantCulture, format, args) + "-\r\n" + Environment.StackTrace;
            Environment.FailFast(message);
        }

        public static void Fail(string format, params object[] args)
        {
            var assertion = args == null || args.Length == 0
                ? format
                : string.Format(
                    CultureInfo.InvariantCulture,
                    format,
                    args);
            throw new AssertFailedException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "Assert Failed: {0}\nStack:{1}",
                    assertion,
                    Environment.StackTrace
                ));
        }

        public static void IsTrue(bool condition, string format, object[] args)
        {
            if (condition)
                return;
            Fail(format, args);
        }

        public static void IsTrue(bool condition, string format)
        {
            if (condition)
                return;
            Fail(format);
        }

        public static void IsTrue<T1>(bool condition, string format, T1 t1)
        {
            if (condition)
                return;
            Fail(format, (object)t1);
        }

        public static void IsTrue<T1, T2>(bool condition, string format, T1 t1, T2 t2)
        {
            if (condition)
                return;
            Fail(format, t1, t2);
        }

        public static void IsTrue<T1, T2, T3>(bool condition, string format, T1 t1, T2 t2, T3 t3)
        {
            if (condition)
                return;
            Fail(format, t1, t2, t3);
        }

        public static void IsTrue<T1, T2, T3, T4>(bool condition, string format, T1 t1, T2 t2, T3 t3, T4 t4)
        {
            if (condition)
                return;
            Fail(format, t1, t2, t3, t4);
        }

        public static void IsTrue<T1, T2, T3, T4, T5>(bool condition, string format, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5)
        {
            if (condition)
                return;
            Fail(format, t1, t2, t3, t4, t5);
        }

        public static void IsTrue<T1, T2, T3, T4, T5, T6>(bool condition, string format, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6)
        {
            if (condition)
                return;
            Fail(format, t1, t2, t3, t4, t5, t6);
        }

        public static void IsTrue<T1, T2, T3, T4, T5, T6, T7>(bool condition, string format, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6, T7 t7)
        {
            if (condition)
                return;
            Fail(format, t1, t2, t3, t4, t5, t6, t7);
        }

        public static void IsTrue<T1, T2, T3, T4, T5, T6, T7, T8>(bool condition, string format, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6, T7 t7, T8 t8)
        {
            if (condition)
                return;
            Fail(format, t1, t2, t3, t4, t5, t6, t7, t8);
        }

        public static void IsTrue(bool condition)
        {
            if (condition)
                return;
            Fail(string.Empty, null);
        }
    }
}