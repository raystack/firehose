# JEXL-based Filters

To enable JEXL-based filtering, you need to set the Firehose environment variable `FILTER_ENGINE=JEXL` and provide the required JEXL filter expression to the variable`FILTER_JEXL_EXPRESSION .`

## JEXL Filter Expression

Filter expressions are JEXL expressions used to filter messages just after reading from Kafka and before sending to Sink.

### Rules to write expressions:

- All the expressions are like a piece of Java code. Follow rules for every data type, as like writing a Java code. Parenthesis `()`can be used to combine multiple arithmetic or logical expressions into a single JEXL expression, which evaluates to a boolean value ie. `true` or `false`
- Start with the object reference of the schema proto class of the key/message on which you wish to apply the filter. Make sure to change the first letter of the proto class to lower case. eg - `sampleLogMessage` \(if `FILTER_SCHEMA_PROTO_CLASS=com.xyz.SampleLogMessage` \)
- Access a particular field by calling the getter method on the proto object. The name of the getter method will be the field name, changed to camel-case, with all underscore \( `_`\) characters removed, and prefixed by the string `get` eg - if the field name is `vehicle_type` , then the getter method name would be `getVehicleType()`
- Access nested fields using linked invocations of the getter methods, `.` and repeatedly call the getter method for the every level of nested field. eg - `sampleLogKey.getEventTimestamp().getSeconds()`
- You can combine multiple fields of the key/message protobuf in a single JEXL expression and perform any arithmetic or logical operations between them. e.g - `sampleKey.getTime().getSeconds() * 1000 + sampleKey.getTime().getMillis() > 22809`

## Syntax

### Literals

Integer Literals
1 or more digits from 0 to 9, eg 42.

Float Literals
1 or more digits from 0 to 9, followed by a decimal point and then one or more digits from 0 to 9, optionally followed by f or F, eg 42.0 or 42.0f.

Long Literals
1 or more digits from 0 to 9 suffixed with l or L , eg 42l.

Double Literals
1 or more digits from 0 to 9, followed by a decimal point and then one or more digits from 0 to 9 suffixed with d or D , eg 42.0d. A special literal NaN can be used to denote Double.NaN constant

String literals
Can start and end with either ' or " delimiters, e.g. "Hello world" and 'Hello world' are equivalent.
The escape character is \ (backslash). Unicode characters can be used in string literals;
Unicode escape sequences consist of:
a backslash '\'
a 'u'
4 hexadecimal digits ([0-9],[A-H],[a-h]).
Such sequences represent the UTF-16 encoding of a Unicode character, for example, 'a' is equivalent to '\u0061'.

Regular expression (regex) literals
Start with ~/ and ends with / delimiters, e.g. ~/ABC.\*/
The escape character is \ (backslash); it only escapes the string delimiter \ (slash)

Boolean literals
The literals true and false can be used, e.g. val1 == true

Null literal
The null value is represented as in java using the literal null, e.g. val1 == null

Array literal
A [ followed by zero or more expressions separated by , and ending with ], e.g. [ 1, 2, "three" ]
This syntax creates an `Object[]`.

Empty array literal can be specified as [] with result of creating Object[]
JEXL will attempt to strongly type the array; if all entries are of the same class or if all entries are Number instance, the array literal will be an MyClass[] in the former case, a Number[] in the latter case.
Furthermore, if all entries in the array literal are of the same class and that class has an equivalent primitive type, the array returned will be a primitive array. e.g. [1, 2, 3] will be interpreted as int[].

List literal
A [ followed by zero or more expressions separated by , and ending with ,...], e.g. [ 1, 2, "three",...]
This syntax creates an `ArrayList<Object>`.
Empty list literal can be specified as [...]

Set literal
A { followed by zero or more expressions separated by , and ending with }, e.g. { "one" , 2, "more"}
This syntax creates a `HashSet<Object>`.
Empty set literal can be specified as {}

**Map literal**
A { followed by zero or more sets of key : value pairs separated by , and ending with }, e.g. { "one" : 1, "two" : 2, "three" : 3, "more": "many more" }
This syntax creates a `HashMap<Object,Object>`.
Empty map literal can be specified as {:}

### Operators

In addition to the common arithmetic and logical operations, the following operators are also available.

Boolean and
The usual && operator can be used as well as the word and, e.g. cond1 and cond2 and cond1 && cond2 are equivalent.
Note that this operator can not be overloaded

Boolean or
The usual || operator can be used as well as the word or, e.g. cond1 or cond2 and cond1 || cond2 are equivalent.
Note that this operator can not be overloaded

Boolean not
The usual ! operator can be used as well as the word not, e.g. !cond1 and not cond1 are equivalent.
Note that this operator can not be overloaded

Bitwise and
The usual & operator is used, e.g. 33 & 4, 0010 0001 & 0000 0100 = 0.

Bitwise or
The usual | operator is used, e.g. 33 | 4, 0010 0001 | 0000 0100 = 0010 0101 = 37.

Bitwise xor
The usual ^ operator is used, e.g. 33 ^ 4, 0010 0001 ^ 0000 0100 = 0010 0100 = 37.

Bitwise complement
The usual ~ operator is used, e.g. ~33, ~0010 0001 = 1101 1110 = -34.

Ternary conditional ?:
The usual ternary conditional operator condition ? if_true : if_false operator can be used as well as the abbreviation value ?: if_false which returns the value if its evaluation is defined, non-null and non-false, e.g. val1 ? val1 : val2 and val1 ?: val2 are equivalent.
NOTE: The condition will evaluate to false when it refers to an undefined variable or null for all JexlEngine flag combinations. This allows explicit syntactic leniency and treats the condition 'if undefined or null or false' the same way in all cases.
Note that this operator can not be overloaded

Null coalescing operator ??
The null coalescing operator returns the result of its first operand if it is defined and is not null.
When xandyare null or undefined, x ?? 'unknown or null x' evaluates as 'unknown or null x' y ?? "default" evaluates as "default".
When var x = 42 and var y = "forty-two",x??"other" evaluates as 42 and y??"other" evaluates as "forty-two".
NOTE: this operator does not behave like the ternary conditional since it does not coerce the first argument to a boolean to evaluate the condition. When var x = false and var y = 0,x??true evaluates as false and y??1 evaluates as 0.
Note that this operator can not be overloaded

Equality
The usual == operator can be used as well as the abbreviation eq. For example val1 == val2 and val1 eq val2 are equivalent.
null is only ever equal to null, that is if you compare null to any non-null value, the result is false.
Equality uses the java equals method

In or Match=~
The syntactically Perl inspired =~ operator can be used to check that a string matches a regular expression (expressed either a Java String or a java.util.regex.Pattern). For example "abcdef" =~ "abc.\* returns true. It also checks whether any collection, set or map (on keys) contains a value or not; in that case, it behaves as an "in" operator. Note that arrays and user classes exposing a public 'contains' method will allow their instances to behave as right-hand side operands of this operator. "a" =~ ["a","b","c","d","e",f"] returns true.

Not-In or Not-Match!~
The syntactically Perl inspired !~ operator can be used to check that a string does not match a regular expression (expressed either a Java String or a java.util.regex.Pattern). For example "abcdef" !~ "abc.\* returns false. It also checks whether any collection, set or map (on keys) does not contain a value; in that case, it behaves as "not in" operator. Note that arrays and user classes exposing a public 'contains' method will allow their instances to behave as right-hand side operands of this operator. "a" !~ ["a","b","c","d","e",f"] returns true.

Starts With=^
The =^ operator is a short-hand for the 'startsWith' method. For example, "abcdef" =^ "abc" returns true. Note that through duck-typing, user classes exposing a public 'startsWith' method will allow their instances to behave as left-hand side operands of this operator.
Not Starts With!^
This is the negation of the 'starts with' operator. a !^ "abc" is equivalent to !(a =^ "abc")

Ends With=$
The =$ operator is a short-hand for the 'endsWith' method. For example, "abcdef" =$ "def" returns true. Note that through duck-typing, user classes exposing an 'endsWith' method will allow their instances to behave as left-hand side operands of this operator.

Not Ends With!$
This is the negation of the 'ends with' operator. a !$ "abc" is equivalent to !(a =$ "abc")

## **Examples**

Sample proto message:

```text
===================KEY==========================
driver_id: "abcde12345"
vehicle_type: BIKE
event_timestamp {
  seconds: 186178
  nanos: 323080
}
driver_status: UNAVAILABLE

================= MESSAGE=======================
driver_id: "abcde12345"
vehicle_type: BIKE
event_timestamp {
  seconds: 186178
  nanos: 323080
}
driver_status: UNAVAILABLE
app_version: "1.0.0"
driver_location {
  latitude: 0.6487193703651428
  longitude: 0.791822075843811
  altitude_in_meters: 0.9949166178703308
  accuracy_in_meters: 0.39277541637420654
  speed_in_meters_per_second: 0.28804516792297363
}
gcm_key: "abc123"
```

_**Key**_-_**based filter expressions examples:**_

- `sampleLogKey.getDriverId()=="abcde12345"`
- `sampleLogKey.getVehicleType()=="BIKE"`
- `sampleLogKey.getEventTimestamp().getSeconds()==186178`
- `sampleLogKey.getDriverId()=="abcde12345"&&sampleLogKey.getVehicleType=="BIKE"` \(multiple conditions example 1\)
- `sampleLogKey.getVehicleType()=="BIKE"||sampleLogKey.getEventTimestamp().getSeconds()==186178` \(multiple conditions example 2\)

_**Message -based filter expressions examples:**_

- `sampleLogMessage.getGcmKey()=="abc123"`
- `sampleLogMessage.getDriverId()=="abcde12345"&&sampleLogMessage.getDriverLocation().getLatitude()>0.6487193703651428`
- `sampleLogMessage.getDriverLocation().getAltitudeInMeters>0.9949166178703308`

_**Note: Use `log` sink for testing the applied filtering**_
