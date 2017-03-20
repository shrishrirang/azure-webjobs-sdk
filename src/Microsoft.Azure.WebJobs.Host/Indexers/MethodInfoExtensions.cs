// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Globalization;
using System.Reflection;

namespace Microsoft.Azure.WebJobs.Host.Indexers
{
    internal static class MethodInfoExtensions
    {
        public static string GetFullName(this MethodInfo methodInfo)
        {
            if (methodInfo == null)
            {
                throw new ArgumentNullException("methodInfo");
            }

            return TryGetCustomName(methodInfo, out string customName)
                ? customName
                : String.Format(CultureInfo.InvariantCulture, "{0}.{1}", methodInfo.DeclaringType.FullName, methodInfo.Name);
        }

        public static string GetShortName(this MethodInfo methodInfo)
        {
            if (methodInfo == null)
            {
                throw new ArgumentNullException("methodInfo");
            }

            return TryGetCustomName(methodInfo, out string customName)
                ? customName
                : String.Format(CultureInfo.InvariantCulture, "{0}.{1}", methodInfo.DeclaringType.Name, methodInfo.Name);
        }

        private static bool TryGetCustomName(MethodInfo methodInfo, out string customName)
        {
            customName = methodInfo.GetCustomAttribute<FunctionNameAttribute>()?.Name;
            return !String.IsNullOrEmpty(customName);
        }
    }
}
