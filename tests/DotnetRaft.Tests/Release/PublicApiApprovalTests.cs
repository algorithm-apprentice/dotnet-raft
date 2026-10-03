using System.Globalization;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;

using DotnetRaft.Protocol;

using Google.Protobuf.Reflection;

namespace DotnetRaft.Tests.Release;

public sealed class PublicApiApprovalTests
{
    [Fact]
    public void ManagedPublicApiMatchesApproval()
    {
        const string fileName = "PublicApi.approved.txt";
        string actual = PublicApiFormatter.Generate(
            typeof(RaftConfig).Assembly);
        VerifyOrRewrite(fileName, actual);
    }

    [Fact]
    public void ProtobufWireApiMatchesApproval()
    {
        const string fileName =
            "ProtocolDescriptor.approved.txt";
        string actual =
            ProtocolDescriptorFormatter.Generate(
                RaftReflection.Descriptor);
        VerifyOrRewrite(fileName, actual);
    }

    [Fact]
    public void FormatterDistinguishesRefReturnsAndRequiredFields()
    {
        string byValue =
            PublicApiFormatter.FormatMethodForTesting(
                typeof(FormatterFixture).GetMethod(
                    nameof(FormatterFixture.ByValue))!);
        string byRef =
            PublicApiFormatter.FormatMethodForTesting(
                typeof(FormatterFixture).GetMethod(
                    nameof(FormatterFixture.ByRef))!);
        string byRefReadonly =
            PublicApiFormatter.FormatMethodForTesting(
                typeof(FormatterFixture).GetMethod(
                    nameof(FormatterFixture.ByRefReadonly))!);

        Assert.DoesNotContain("ref ", byValue);
        Assert.Contains("ref int", byRef);
        Assert.Contains(
            "ref readonly int",
            byRefReadonly);
        Assert.Equal(
            "required",
            ProtocolDescriptorFormatter
                .FormatLabelForTesting(
                    isMap: false,
                    isRepeated: false,
                    isRequired: true));
    }

    private static void VerifyOrRewrite(
        string fileName,
        string actual)
    {
        string outputPath = Path.Combine(
            ReleaseTestPaths.OutputReleaseDirectory,
            fileName);
        Assert.True(
            File.Exists(outputPath),
            $"Missing approved API file {outputPath}.");
        Assert.Equal(
            File.ReadAllText(outputPath)
                .Replace(
                    "\r\n",
                    "\n",
                    StringComparison.Ordinal),
            actual);
    }

    private static class FormatterFixture
    {
        private static int _value;

        public static int ByValue()
        {
            return _value;
        }

        public static ref int ByRef()
        {
            return ref _value;
        }

        public static ref readonly int ByRefReadonly()
        {
            return ref _value;
        }
    }
}

internal static class PublicApiFormatter
{
    private static readonly NullabilityInfoContext
        Nullability = new();

    internal static string Generate(Assembly assembly)
    {
        var builder = new StringBuilder();
        foreach (Type type in assembly
                     .GetExportedTypes()
                     .OrderBy(
                         type => type.FullName,
                         StringComparer.Ordinal))
        {
            AppendType(builder, type);
        }

        return builder.ToString();
    }

    internal static string FormatMethodForTesting(
        MethodInfo method)
    {
        return FormatMethod(method);
    }

    private static void AppendType(
        StringBuilder builder,
        Type type)
    {
        builder.Append("type ");
        AppendAttributes(builder, type.CustomAttributes);
        builder.Append("public ");
        if (type.IsAbstract && type.IsSealed)
        {
            builder.Append("static ");
        }
        else
        {
            if (type.IsAbstract && !type.IsInterface)
            {
                builder.Append("abstract ");
            }

            if (type.IsSealed
                && !type.IsValueType
                && !typeof(MulticastDelegate)
                    .IsAssignableFrom(type))
            {
                builder.Append("sealed ");
            }
        }

        builder.Append(GetTypeKind(type));
        builder.Append(' ');
        builder.Append(FormatTypeDefinition(type));
        AppendGenericConstraints(
            builder,
            type.GetGenericArguments()
                .Where(argument =>
                    argument.IsGenericParameter));

        var inheritance = new List<string>();
        if (type.BaseType is not null
            && type.BaseType != typeof(object)
            && type.BaseType != typeof(ValueType)
            && type.BaseType != typeof(Enum)
            && type.BaseType != typeof(MulticastDelegate))
        {
            inheritance.Add(FormatType(type.BaseType));
        }

        inheritance.AddRange(
            type.GetInterfaces()
                .Select(FormatType)
                .Order(StringComparer.Ordinal));
        if (inheritance.Count > 0)
        {
            builder.Append(" : ");
            builder.AppendJoin(", ", inheritance);
        }

        builder.Append('\n');

        const BindingFlags declaredPublic =
            BindingFlags.Public
            | BindingFlags.Instance
            | BindingFlags.Static
            | BindingFlags.DeclaredOnly;
        var members = new List<string>();
        members.AddRange(
            type.GetConstructors(declaredPublic)
                .Select(FormatConstructor));
        members.AddRange(
            type.GetMethods(declaredPublic)
                .Where(method =>
                    !IsAccessor(method))
                .Select(FormatMethod));
        members.AddRange(
            type.GetProperties(declaredPublic)
                .Where(property =>
                    IsPublic(property.GetMethod)
                    || IsPublic(property.SetMethod))
                .Select(FormatProperty));
        members.AddRange(
            type.GetEvents(declaredPublic)
                .Where(@event =>
                    IsPublic(@event.AddMethod)
                    || IsPublic(@event.RemoveMethod))
                .Select(FormatEvent));
        members.AddRange(
            type.GetFields(declaredPublic)
                .Select(FormatField));

        foreach (string member in members
                     .Order(StringComparer.Ordinal))
        {
            builder.Append("  ");
            builder.Append(member);
            builder.Append('\n');
        }

        builder.Append('\n');
    }

    private static string FormatConstructor(
        ConstructorInfo constructor)
    {
        var builder = new StringBuilder();
        AppendAttributes(
            builder,
            constructor.CustomAttributes);
        builder.Append("public ");
        builder.Append(
            constructor.DeclaringType!.Name
                .Split('`')[0]);
        AppendParameters(
            builder,
            constructor.GetParameters());
        return builder.ToString();
    }

    private static string FormatMethod(MethodInfo method)
    {
        var builder = new StringBuilder();
        AppendAttributes(builder, method.CustomAttributes);
        builder.Append("public ");
        AppendMethodModifiers(builder, method);
        AppendReturnModifier(
            builder,
            method.ReturnParameter,
            method.ReturnType);
        Type returnType = method.ReturnType.IsByRef
            ? method.ReturnType.GetElementType()!
            : method.ReturnType;
        builder.Append(
            FormatNullability(
                returnType,
                Nullability.Create(
                    method.ReturnParameter)));
        AppendCustomModifiers(
            builder,
            method.ReturnParameter);
        builder.Append(' ');
        builder.Append(method.Name);
        if (method.IsGenericMethodDefinition)
        {
            builder.Append('<');
            builder.AppendJoin(
                ", ",
                method.GetGenericArguments()
                    .Select(FormatGenericParameter));
            builder.Append('>');
        }

        AppendParameters(
            builder,
            method.GetParameters());
        AppendGenericConstraints(
            builder,
            method.GetGenericArguments()
                .Where(argument =>
                    argument.IsGenericParameter));
        return builder.ToString();
    }

    private static string FormatProperty(
        PropertyInfo property)
    {
        var builder = new StringBuilder();
        AppendAttributes(
            builder,
            property.CustomAttributes);
        MethodInfo? publicAccessor =
            IsPublic(property.GetMethod)
                ? property.GetMethod
                : property.SetMethod;
        builder.Append("public ");
        if (publicAccessor?.IsStatic == true)
        {
            builder.Append("static ");
        }

        MethodInfo? getter = property.GetMethod;
        if (getter is not null)
        {
            AppendReturnModifier(
                builder,
                getter.ReturnParameter,
                property.PropertyType);
        }

        Type propertyType =
            property.PropertyType.IsByRef
                ? property.PropertyType
                    .GetElementType()!
                : property.PropertyType;
        builder.Append(
            FormatNullability(
                propertyType,
                Nullability.Create(property)));
        builder.Append(' ');
        builder.Append(property.Name);
        ParameterInfo[] indexes =
            property.GetIndexParameters();
        if (indexes.Length > 0)
        {
            builder.Append('[');
            builder.AppendJoin(
                ", ",
                indexes.Select(FormatParameter));
            builder.Append(']');
        }

        builder.Append(" { ");
        if (property.GetMethod is not null)
        {
            builder.Append(
                FormatAccessor(
                    property.GetMethod,
                    "get"));
            builder.Append("; ");
        }

        if (property.SetMethod is not null)
        {
            bool init = property.SetMethod
                .ReturnParameter
                .GetRequiredCustomModifiers()
                .Contains(
                    typeof(IsExternalInit));
            builder.Append(
                FormatAccessor(
                    property.SetMethod,
                    init ? "init" : "set"));
            builder.Append("; ");
        }

        builder.Append('}');
        return builder.ToString();
    }

    private static string FormatEvent(EventInfo @event)
    {
        var builder = new StringBuilder();
        AppendAttributes(
            builder,
            @event.CustomAttributes);
        builder.Append("public ");
        MethodInfo? accessor =
            @event.AddMethod
            ?? @event.RemoveMethod;
        if (accessor?.IsStatic == true)
        {
            builder.Append("static ");
        }

        builder.Append("event ");
        builder.Append(
            FormatType(
                @event.EventHandlerType
                ?? typeof(void)));
        builder.Append(' ');
        builder.Append(@event.Name);
        builder.Append(" { ");
        if (@event.AddMethod is not null)
        {
            builder.Append(
                FormatAccessor(
                    @event.AddMethod,
                    "add"));
            builder.Append("; ");
        }

        if (@event.RemoveMethod is not null)
        {
            builder.Append(
                FormatAccessor(
                    @event.RemoveMethod,
                    "remove"));
            builder.Append("; ");
        }

        builder.Append('}');
        return builder.ToString();
    }

    private static string FormatField(FieldInfo field)
    {
        var builder = new StringBuilder();
        AppendAttributes(builder, field.CustomAttributes);
        builder.Append("public ");
        if (field.IsLiteral)
        {
            builder.Append("const ");
        }
        else
        {
            if (field.IsStatic)
            {
                builder.Append("static ");
            }

            if (field.IsInitOnly)
            {
                builder.Append("readonly ");
            }
        }

        builder.Append(FormatType(field.FieldType));
        builder.Append(' ');
        builder.Append(field.Name);
        if (field.IsLiteral)
        {
            builder.Append(" = ");
            builder.Append(
                FormatConstant(
                    field.GetRawConstantValue(),
                    field.FieldType));
        }

        return builder.ToString();
    }

    private static void AppendMethodModifiers(
        StringBuilder builder,
        MethodInfo method)
    {
        if (method.IsStatic)
        {
            builder.Append("static ");
            return;
        }

        if (method.IsAbstract)
        {
            builder.Append("abstract ");
        }
        else if (method.IsVirtual)
        {
            builder.Append(
                method.IsFinal
                    ? "sealed virtual "
                    : "virtual ");
        }
    }

    private static void AppendReturnModifier(
        StringBuilder builder,
        ParameterInfo returnParameter,
        Type returnType)
    {
        if (!returnType.IsByRef)
        {
            return;
        }

        bool readOnly = returnParameter.CustomAttributes
            .Any(attribute =>
                attribute.AttributeType.FullName
                == "System.Runtime.CompilerServices.IsReadOnlyAttribute")
            || returnParameter
                .GetRequiredCustomModifiers()
                .Any(modifier =>
                    modifier.FullName
                    == "System.Runtime.CompilerServices.IsReadOnlyAttribute");
        builder.Append(
            readOnly
                ? "ref readonly "
                : "ref ");
    }

    private static void AppendParameters(
        StringBuilder builder,
        ParameterInfo[] parameters)
    {
        builder.Append('(');
        builder.AppendJoin(
            ", ",
            parameters.Select(FormatParameter));
        builder.Append(')');
    }

    private static string FormatParameter(
        ParameterInfo parameter)
    {
        var builder = new StringBuilder();
        AppendAttributes(
            builder,
            parameter.CustomAttributes);
        Type type = parameter.ParameterType;
        if (parameter.GetCustomAttribute<
                ParamArrayAttribute>() is not null)
        {
            builder.Append("params ");
        }
        else if (type.IsByRef)
        {
            builder.Append(
                parameter.IsOut
                    ? "out "
                    : parameter.IsIn
                        ? "in "
                        : "ref ");
            type = type.GetElementType()!;
        }

        builder.Append(
            FormatNullability(
                type,
                Nullability.Create(parameter)));
        AppendCustomModifiers(builder, parameter);
        builder.Append(' ');
        builder.Append(parameter.Name);
        if (parameter.HasDefaultValue)
        {
            builder.Append(" = ");
            builder.Append(
                FormatConstant(
                    parameter.DefaultValue,
                    type));
        }

        return builder.ToString();
    }

    private static void AppendCustomModifiers(
        StringBuilder builder,
        ParameterInfo parameter)
    {
        foreach (Type modifier in parameter
                     .GetRequiredCustomModifiers()
                     .OrderBy(
                         type => type.FullName,
                         StringComparer.Ordinal))
        {
            builder.Append(" modreq(");
            builder.Append(FormatType(modifier));
            builder.Append(')');
        }

        foreach (Type modifier in parameter
                     .GetOptionalCustomModifiers()
                     .OrderBy(
                         type => type.FullName,
                         StringComparer.Ordinal))
        {
            builder.Append(" modopt(");
            builder.Append(FormatType(modifier));
            builder.Append(')');
        }
    }

    private static void AppendGenericConstraints(
        StringBuilder builder,
        IEnumerable<Type> genericParameters)
    {
        foreach (Type parameter in genericParameters)
        {
            var constraints = new List<string>();
            GenericParameterAttributes attributes =
                parameter.GenericParameterAttributes;
            GenericParameterAttributes special =
                attributes
                & GenericParameterAttributes
                    .SpecialConstraintMask;
            if (special.HasFlag(
                    GenericParameterAttributes
                        .ReferenceTypeConstraint))
            {
                constraints.Add("class");
            }

            if (special.HasFlag(
                    GenericParameterAttributes
                        .NotNullableValueTypeConstraint))
            {
                constraints.Add("struct");
            }

            constraints.AddRange(
                parameter.GetGenericParameterConstraints()
                    .Select(FormatType)
                    .Order(StringComparer.Ordinal));
            if (special.HasFlag(
                    GenericParameterAttributes
                        .DefaultConstructorConstraint))
            {
                constraints.Add("new()");
            }

            if (constraints.Count > 0)
            {
                builder.Append(" where ");
                builder.Append(parameter.Name);
                builder.Append(" : ");
                builder.AppendJoin(", ", constraints);
            }
        }
    }

    private static string FormatGenericParameter(
        Type parameter)
    {
        GenericParameterAttributes variance =
            parameter.GenericParameterAttributes
            & GenericParameterAttributes.VarianceMask;
        return variance switch
        {
            GenericParameterAttributes.Covariant =>
                "out " + parameter.Name,
            GenericParameterAttributes.Contravariant =>
                "in " + parameter.Name,
            _ => parameter.Name,
        };
    }

    private static string FormatTypeDefinition(Type type)
    {
        string name = (type.FullName ?? type.Name)
            .Replace('+', '.');
        int tick = name.IndexOf('`');
        if (tick >= 0)
        {
            name = name[..tick];
        }

        Type[] arguments = type.GetGenericArguments()
            .Where(argument =>
                argument.DeclaringType == type
                || argument.DeclaringMethod is not null)
            .ToArray();
        if (arguments.Length > 0)
        {
            name += "<"
                + string.Join(
                    ", ",
                    arguments.Select(
                        FormatGenericParameter))
                + ">";
        }

        return name;
    }

    private static string FormatType(Type type)
    {
        return FormatNullability(type, null);
    }

    private static string FormatNullability(
        Type type,
        NullabilityInfo? nullability)
    {
        if (type.IsByRef)
        {
            type = type.GetElementType()!;
        }

        if (type.IsArray)
        {
            string element = FormatNullability(
                type.GetElementType()!,
                nullability?.ElementType);
            return element
                + "["
                + new string(
                    ',',
                    type.GetArrayRank() - 1)
                + "]"
                + NullableSuffix(type, nullability);
        }

        Type? nullable =
            Nullable.GetUnderlyingType(type);
        if (nullable is not null)
        {
            return FormatType(nullable) + "?";
        }

        if (type.IsGenericParameter)
        {
            return type.Name
                + NullableSuffix(type, nullability);
        }

        if (type.IsGenericType)
        {
            Type definition =
                type.GetGenericTypeDefinition();
            string name =
                (definition.FullName
                 ?? definition.Name)
                .Replace('+', '.');
            int tick = name.IndexOf('`');
            if (tick >= 0)
            {
                name = name[..tick];
            }

            Type[] arguments =
                type.GetGenericArguments();
            string[] formatted =
                new string[arguments.Length];
            for (var index = 0;
                 index < arguments.Length;
                 index++)
            {
                NullabilityInfo? info =
                    nullability is not null
                    && index
                        < nullability
                            .GenericTypeArguments
                            .Length
                        ? nullability
                            .GenericTypeArguments[index]
                        : null;
                formatted[index] =
                    FormatNullability(
                        arguments[index],
                        info);
            }

            return name
                + "<"
                + string.Join(", ", formatted)
                + ">"
                + NullableSuffix(type, nullability);
        }

        string alias = type == typeof(void)
            ? "void"
            : type == typeof(bool)
                ? "bool"
                : type == typeof(byte)
                    ? "byte"
                    : type == typeof(sbyte)
                        ? "sbyte"
                        : type == typeof(short)
                            ? "short"
                            : type == typeof(ushort)
                                ? "ushort"
                                : type == typeof(int)
                                    ? "int"
                                    : type == typeof(uint)
                                        ? "uint"
                                        : type == typeof(long)
                                            ? "long"
                                            : type == typeof(ulong)
                                                ? "ulong"
                                                : type == typeof(float)
                                                    ? "float"
                                                    : type == typeof(double)
                                                        ? "double"
                                                        : type == typeof(decimal)
                                                            ? "decimal"
                                                            : type == typeof(char)
                                                                ? "char"
                                                                : type == typeof(string)
                                                                    ? "string"
                                                                    : type == typeof(object)
                                                                        ? "object"
                                                                        : (type.FullName
                                                                           ?? type.Name)
                                                                        .Replace(
                                                                            '+',
                                                                            '.');
        return alias + NullableSuffix(type, nullability);
    }

    private static string NullableSuffix(
        Type type,
        NullabilityInfo? nullability)
    {
        return !type.IsValueType
            && nullability?.ReadState
                == NullabilityState.Nullable
                ? "?"
                : string.Empty;
    }

    private static string FormatConstant(
        object? value,
        Type declaredType)
    {
        if (value is null
            || value == DBNull.Value
            || value == Missing.Value)
        {
            return declaredType.IsValueType
                ? $"default({FormatType(declaredType)})"
                : "null";
        }

        if (declaredType.IsEnum)
        {
            return Convert.ToUInt64(
                    value,
                    CultureInfo.InvariantCulture)
                .ToString(
                    CultureInfo.InvariantCulture);
        }

        return value switch
        {
            string text =>
                JsonSerializer.Serialize(text),
            char character =>
                "'"
                + character.ToString()
                    .Replace(
                        "'",
                        "\\'",
                        StringComparison.Ordinal)
                + "'",
            bool boolean =>
                boolean ? "true" : "false",
            float number when float.IsNaN(number) =>
                "float.NaN",
            double number when double.IsNaN(number) =>
                "double.NaN",
            IFormattable formattable =>
                formattable.ToString(
                    null,
                    CultureInfo.InvariantCulture),
            _ => value.ToString()
                 ?? string.Empty,
        };
    }

    private static string FormatAccessor(
        MethodInfo accessor,
        string keyword)
    {
        return IsPublic(accessor)
            ? keyword
            : GetVisibility(accessor)
                + " "
                + keyword;
    }

    private static string GetVisibility(
        MethodBase method)
    {
        if (method.IsPublic)
        {
            return "public";
        }

        if (method.IsFamilyAndAssembly)
        {
            return "private protected";
        }

        if (method.IsFamilyOrAssembly)
        {
            return "protected internal";
        }

        if (method.IsFamily)
        {
            return "protected";
        }

        if (method.IsAssembly)
        {
            return "internal";
        }

        return "private";
    }

    private static bool IsPublic(MethodInfo? method)
    {
        return method?.IsPublic == true;
    }

    private static bool IsAccessor(MethodInfo method)
    {
        return method.IsSpecialName
            && (method.Name.StartsWith(
                    "get_",
                    StringComparison.Ordinal)
                || method.Name.StartsWith(
                    "set_",
                    StringComparison.Ordinal)
                || method.Name.StartsWith(
                    "add_",
                    StringComparison.Ordinal)
                || method.Name.StartsWith(
                    "remove_",
                    StringComparison.Ordinal));
    }

    private static string GetTypeKind(Type type)
    {
        if (typeof(MulticastDelegate)
            .IsAssignableFrom(type.BaseType))
        {
            return "delegate";
        }

        if (type.IsEnum)
        {
            return "enum";
        }

        if (type.IsInterface)
        {
            return "interface";
        }

        if (type.IsValueType)
        {
            return "struct";
        }

        return "class";
    }

    private static void AppendAttributes(
        StringBuilder builder,
        IEnumerable<CustomAttributeData> attributes)
    {
        foreach (CustomAttributeData attribute in
                 attributes
                     .Where(IsRelevantAttribute)
                     .OrderBy(
                         attribute =>
                             attribute.AttributeType.FullName,
                         StringComparer.Ordinal))
        {
            builder.Append('[');
            builder.Append(
                attribute.AttributeType.FullName);
            var values = new List<string>();
            values.AddRange(
                attribute.ConstructorArguments
                    .Select(FormatAttributeValue));
            values.AddRange(
                attribute.NamedArguments
                    .OrderBy(
                        argument =>
                            argument.MemberName,
                        StringComparer.Ordinal)
                    .Select(argument =>
                        argument.MemberName
                        + "="
                        + FormatAttributeValue(
                            argument.TypedValue)));
            if (values.Count > 0)
            {
                builder.Append('(');
                builder.AppendJoin(", ", values);
                builder.Append(')');
            }

            builder.Append("] ");
        }
    }

    private static bool IsRelevantAttribute(
        CustomAttributeData attribute)
    {
        return attribute.AttributeType.FullName is
            "System.ObsoleteAttribute"
            or "System.FlagsAttribute"
            or "System.Runtime.CompilerServices.ExtensionAttribute"
            or "System.ParamArrayAttribute"
            or "System.Runtime.CompilerServices.RequiredMemberAttribute"
            or "System.Diagnostics.CodeAnalysis.SetsRequiredMembersAttribute";
    }

    private static string FormatAttributeValue(
        CustomAttributeTypedArgument argument)
    {
        if (argument.Value is
            IReadOnlyCollection<CustomAttributeTypedArgument>
                collection)
        {
            return "["
                + string.Join(
                    ", ",
                    collection.Select(
                        FormatAttributeValue))
                + "]";
        }

        return FormatConstant(
            argument.Value,
            argument.ArgumentType);
    }
}

internal static class ProtocolDescriptorFormatter
{
    internal static string Generate(
        FileDescriptor descriptor)
    {
        var builder = new StringBuilder();
        builder.Append("file ")
            .Append(descriptor.Name)
            .Append(" package=")
            .Append(descriptor.Package)
            .Append('\n');
        foreach (EnumDescriptor @enum in
                 descriptor.EnumTypes.OrderBy(
                     value => value.FullName,
                     StringComparer.Ordinal))
        {
            AppendEnum(builder, @enum);
        }

        foreach (MessageDescriptor message in
                 descriptor.MessageTypes.OrderBy(
                     value => value.FullName,
                     StringComparer.Ordinal))
        {
            AppendMessage(builder, message);
        }

        return builder.ToString();
    }

    private static void AppendEnum(
        StringBuilder builder,
        EnumDescriptor descriptor)
    {
        builder.Append("enum ")
            .Append(descriptor.FullName)
            .Append('\n');
        foreach (EnumValueDescriptor value in
                 descriptor.Values.OrderBy(
                     item => item.Number))
        {
            builder.Append("  ")
                .Append(value.Name)
                .Append('=')
                .Append(value.Number)
                .Append('\n');
        }
    }

    private static void AppendMessage(
        StringBuilder builder,
        MessageDescriptor descriptor)
    {
        builder.Append("message ")
            .Append(descriptor.FullName)
            .Append('\n');
        foreach (FieldDescriptor field in
                 descriptor.Fields
                     .InFieldNumberOrder())
        {
            builder.Append("  ")
                .Append(field.FieldNumber)
                .Append(' ')
                .Append(
                    FormatLabel(
                        field.IsMap,
                        field.IsRepeated,
                        field.IsRequired))
                .Append(' ')
                .Append(field.Name)
                .Append(" type=")
                .Append(field.FieldType);
            if (field.FieldType == FieldType.Enum)
            {
                builder.Append('(')
                    .Append(field.EnumType.FullName)
                    .Append(')');
            }
            else if (field.FieldType is
                     FieldType.Message or
                     FieldType.Group)
            {
                builder.Append('(')
                    .Append(field.MessageType.FullName)
                    .Append(')');
            }

            builder.Append(" wire=")
                .Append(GetWireType(field.FieldType))
                .Append(" packed=")
                .Append(field.IsPacked ? "true" : "false")
                .Append('\n');
        }

        foreach (EnumDescriptor @enum in
                 descriptor.EnumTypes.OrderBy(
                     value => value.FullName,
                     StringComparer.Ordinal))
        {
            AppendEnum(builder, @enum);
        }

        foreach (MessageDescriptor nested in
                 descriptor.NestedTypes.OrderBy(
                     value => value.FullName,
                     StringComparer.Ordinal))
        {
            AppendMessage(builder, nested);
        }
    }

    private static string GetWireType(
        FieldType fieldType)
    {
        return fieldType switch
        {
            FieldType.Double
                or FieldType.Fixed64
                or FieldType.SFixed64 => "fixed64",
            FieldType.Float
                or FieldType.Fixed32
                or FieldType.SFixed32 => "fixed32",
            FieldType.String
                or FieldType.Bytes
                or FieldType.Message => "length-delimited",
            FieldType.Group => "start-group",
            _ => "varint",
        };
    }

    internal static string FormatLabelForTesting(
        bool isMap,
        bool isRepeated,
        bool isRequired)
    {
        return FormatLabel(
            isMap,
            isRepeated,
            isRequired);
    }

    private static string FormatLabel(
        bool isMap,
        bool isRepeated,
        bool isRequired)
    {
        if (isMap)
        {
            return "map";
        }

        if (isRepeated)
        {
            return "repeated";
        }

        return isRequired
            ? "required"
            : "optional";
    }
}
