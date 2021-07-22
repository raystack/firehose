# Using Filters

## Filter Variables

The following shell variables need to be set in the environment or in `env/local.properties` file in order to apply filters to incoming messages in Firehose

| **Variable Name**  | **Description**  |
| :--- | :--- |
| `FILTER_JEXL_DATA_SOURCE` | `key`/`message`/`none`depending on where to apply filter  |
| `FILTER_JEXL_EXPRESSION`  |  JEXL filter expression   |
| `FILTER_JEXL_SCHEMA_PROTO_CLASS` | The fully qualified name of the proto schema so that the key/message in Kafka could be parsed.  |

Example:

`FILTER_JEXL_DATA_SOURCE=key`

`FILTER_JEXL_EXPRESSION=driverLocationLogKey.getVehicleType()=="BIKE"`

`FILTER_JEXL_SCHEMA_PROTO_CLASS=com.gojek.esb.driverlocation.DriverLocationLogKey`

## Filter Expressions

Filter expressions are JEXL expressions used to filter messages just after reading from Kafka and before sending to Sink.

### Rules to write expressions:

* All the expressions are like a piece of Java code. Follow rules for every data type, as like writing a Java code. Parenthesis `()`can be used to combine multiple arithmetic or logical expressions into a single JEXL expression, which evaluates to a boolean value ie. `true` or `false`



* Start with the object reference of the schema proto class of the key/message on which you wish to apply the filter. Make sure to change the first letter of the proto class to lower case.                                                                                                                                                               eg - `sampleLogMessage`  \(if `FILTER_JEXL_SCHEMA_PROTO_CLASS=com.xyz.SampleLogMessage` \)



* Access a particular field by calling the getter method on the proto object. The name of the getter method will be the field name, changed to camel-case, with all underscore \( `_`\) characters removed, and prefixed by the string `get`                                                                                                                                    eg - if the field name is `vehicle_type` , then the getter method name would be `getVehicleType()`



* Access nested fields using linked invocations of the getter methods, `.`  and repeatedly call the getter method for the every level of nested field.                                                                                                                            eg - `sampleLogKey.getEventTimestamp().getSeconds()`    



* You can combine multiple fields of the key/message protobuf in a single JEXL expression and perform any arithmetic or logical operations between them.                                                                                                                                    e.g - `sampleKey.getTime().getSeconds() * 1000 + sampleKey.getTime().getMillis() > 22809`

## Syntax

### Literals

<table>
  <thead>
    <tr>
      <th style="text-align:left">Item</th>
      <th style="text-align:left">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align:left">Integer Literals</td>
      <td style="text-align:left">1 or more digits from <code>0</code> to <code>9</code>, eg <code>42</code>.</td>
    </tr>
    <tr>
      <td style="text-align:left">Float Literals</td>
      <td style="text-align:left">1 or more digits from <code>0</code> to <code>9</code>, followed by a decimal
        point and then one or more digits from <code>0</code> to <code>9</code>, optionally
        followed by <code>f</code> or <code>F</code>, eg <code>42.0</code> or <code>42.0f</code>.</td>
    </tr>
    <tr>
      <td style="text-align:left">Long Literals</td>
      <td style="text-align:left">1 or more digits from <code>0</code> to <code>9</code> suffixed with <code>l</code> or <code>L</code> ,
        eg <code>42l</code>.</td>
    </tr>
    <tr>
      <td style="text-align:left">Double Literals</td>
      <td style="text-align:left">1 or more digits from <code>0</code> to <code>9</code>, followed by a decimal
        point and then one or more digits from <code>0</code> to <code>9</code> suffixed
        with <code>d</code> or <code>D</code> , eg <code>42.0d</code>. A special literal <code>NaN</code> can
        be used to denote <code>Double.NaN</code> constant</td>
    </tr>
    <tr>
      <td style="text-align:left">String literals</td>
      <td style="text-align:left">
        <p>Can start and end with either <code>&apos;</code> or <code>&quot;</code> delimiters,
          e.g. <code>&quot;Hello world&quot;</code> and <code>&apos;Hello world&apos;</code> are
          equivalent.</p>
        <p>The escape character is <code>\</code> (backslash). Unicode characters can
          be used in string literals;</p>
        <ul>
          <li>Unicode escape sequences consist of:</li>
          <li>a backslash &apos;\&apos;</li>
          <li>a &apos;u&apos;</li>
          <li>4 hexadecimal digits ([0-9],[A-H],[a-h]).</li>
        </ul>
        <p>Such sequences represent the UTF-16 encoding of a Unicode character, for
          example, <code>&apos;a&apos;</code> is equivalent to <code>&apos;\u0061&apos;</code>.</p>
      </td>
    </tr>
    <tr>
      <td style="text-align:left">Regular expression (regex) literals</td>
      <td style="text-align:left">
        <p>Start with <code>~/</code> and ends with <code>/</code> delimiters, e.g. <code>~/ABC.*/</code>
        </p>
        <p>The escape character is <code>\</code> (backslash); it only escapes the
          string delimiter <code>\</code> (slash)</p>
      </td>
    </tr>
    <tr>
      <td style="text-align:left">Boolean literals</td>
      <td style="text-align:left">The literals <code>true</code> and <code>false</code> can be used, e.g. <code>val1 == true</code>
      </td>
    </tr>
    <tr>
      <td style="text-align:left">Null literal</td>
      <td style="text-align:left">The null value is represented as in java using the literal <code>null</code>,
        e.g. <code>val1 == null</code>
      </td>
    </tr>
    <tr>
      <td style="text-align:left">Array literal</td>
      <td style="text-align:left">
        <p>A <code>[</code> followed by zero or more expressions separated by <code>,</code> and
          ending with <code>]</code>, e.g. <code>[ 1, 2, &quot;three&quot; ]</code>
        </p>
        <p>This syntax creates an <code>Object[]</code>.</p>
        <p>Empty array literal can be specified as <code>[]</code> with result of creating <code>Object[]</code>
        </p>
        <p>JEXL will attempt to strongly type the array; if all entries are of the
          same class or if all entries are Number instance, the array literal will
          be an <code>MyClass[]</code> in the former case, a <code>Number[]</code> in
          the latter case.</p>
        <p>Furthermore, if all entries in the array literal are of the same class
          and that class has an equivalent primitive type, the array returned will
          be a primitive array. e.g. <code>[1, 2, 3]</code> will be interpreted as <code>int[]</code>.</p>
      </td>
    </tr>
    <tr>
      <td style="text-align:left">List literal</td>
      <td style="text-align:left">
        <p>A <code>[</code> followed by zero or more expressions separated by <code>,</code> and
          ending with <code>,...]</code>, e.g. <code>[ 1, 2, &quot;three&quot;,...]</code>
        </p>
        <p>This syntax creates an <code>ArrayList&lt;Object&gt;</code>.</p>
        <p>Empty list literal can be specified as <code>[...]</code>
        </p>
      </td>
    </tr>
    <tr>
      <td style="text-align:left">Set literal</td>
      <td style="text-align:left">
        <p>A <code>{</code> followed by zero or more expressions separated by <code>,</code> and
          ending with <code>}</code>, e.g. <code>{ &quot;one&quot; , 2, &quot;more&quot;}</code>
        </p>
        <p>This syntax creates a <code>HashSet&lt;Object&gt;</code>.</p>
        <p>Empty set literal can be specified as <code>{}</code>
        </p>
      </td>
    </tr>
    <tr>
      <td style="text-align:left">Map literal</td>
      <td style="text-align:left">
        <p>A <code>{</code> followed by zero or more sets of <code>key : value</code> pairs
          separated by <code>,</code> and ending with <code>}</code>, e.g. <code>{ &quot;one&quot; : 1, &quot;two&quot; : 2, &quot;three&quot; : 3, &quot;more&quot;: &quot;many more&quot; }</code>
        </p>
        <p>This syntax creates a <code>HashMap&lt;Object,Object&gt;</code>.</p>
        <p>Empty map literal can be specified as <code>{:}</code>
        </p>
      </td>
    </tr>
  </tbody>
</table>



### Operators

In addition to the common arithmetic and logical operations, the following operators are also available.

<table>
  <thead>
    <tr>
      <th style="text-align:left">Operator</th>
      <th style="text-align:left">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align:left">Boolean <code>and</code>
      </td>
      <td style="text-align:left">
        <p>The usual <code>&amp;&amp;</code> operator can be used as well as the word <code>and</code>,
          e.g. <code>cond1 and cond2</code> and <code>cond1 &amp;&amp; cond2</code> are
          equivalent.</p>
        <p>Note that this operator can not be overloaded</p>
      </td>
    </tr>
    <tr>
      <td style="text-align:left">Boolean <code>or</code>
      </td>
      <td style="text-align:left">
        <p>The usual <code>||</code> operator can be used as well as the word <code>or</code>,
          e.g. <code>cond1 or cond2</code> and <code>cond1 || cond2</code> are equivalent.</p>
        <p>Note that this operator can not be overloaded</p>
      </td>
    </tr>
    <tr>
      <td style="text-align:left">Boolean <code>not</code>
      </td>
      <td style="text-align:left">
        <p>The usual <code>!</code> operator can be used as well as the word <code>not</code>,
          e.g. <code>!cond1</code> and <code>not cond1</code> are equivalent.</p>
        <p>Note that this operator can not be overloaded</p>
      </td>
    </tr>
    <tr>
      <td style="text-align:left">Bitwise <code>and</code>
      </td>
      <td style="text-align:left">The usual <code>&amp;</code> operator is used, e.g. <code>33 &amp; 4</code>,
        0010 0001 &amp; 0000 0100 = 0.</td>
    </tr>
    <tr>
      <td style="text-align:left">Bitwise <code>or</code>
      </td>
      <td style="text-align:left">The usual <code>|</code> operator is used, e.g. <code>33 | 4</code>, 0010
        0001 | 0000 0100 = 0010 0101 = 37.</td>
    </tr>
    <tr>
      <td style="text-align:left">Bitwise <code>xor</code>
      </td>
      <td style="text-align:left">The usual <code>^</code> operator is used, e.g. <code>33 ^ 4</code>, 0010
        0001 ^ 0000 0100 = 0010 0100 = 37.</td>
    </tr>
    <tr>
      <td style="text-align:left">Bitwise <code>complement</code>
      </td>
      <td style="text-align:left">The usual <code>~</code> operator is used, e.g. <code>~33</code>, ~0010 0001
        = 1101 1110 = -34.</td>
    </tr>
    <tr>
      <td style="text-align:left">Ternary conditional <code>?:</code>
      </td>
      <td style="text-align:left">
        <p>The usual ternary conditional operator <code>condition ? if_true : if_false</code> operator
          can be used as well as the abbreviation <code>value ?: if_false</code> which
          returns the <code>value</code> if its evaluation is defined, non-null and
          non-false, e.g. <code>val1 ? val1 : val2</code> and <code>val1 ?: val2 </code>are
          equivalent.</p>
        <p><b>NOTE:</b> The condition will evaluate to <code>false</code> when it refers
          to an undefined variable or <code>null</code> for all <code>JexlEngine</code> flag
          combinations. This allows explicit syntactic leniency and treats the condition
          &apos;if undefined or null or false&apos; the same way in all cases.</p>
        <p>Note that this operator can not be overloaded</p>
      </td>
    </tr>
    <tr>
      <td style="text-align:left">Null coalescing operator <code>??</code>
      </td>
      <td style="text-align:left">
        <p>The null coalescing operator returns the result of its first operand if
          it is defined and is not null.</p>
        <p>When <code>x</code>and<code>y</code>are null or undefined, <code>x ?? &apos;unknown or null x&apos;</code> evaluates
          as <code>&apos;unknown or null x&apos;</code>  <code>y ?? &quot;default&quot;</code> evaluates
          as <code>&quot;default&quot;</code>.</p>
        <p>When <code>var x = 42</code> and <code>var y = &quot;forty-two&quot;</code>,<code>x??&quot;other&quot;</code> evaluates
          as <code>42</code> and <code>y??&quot;other&quot;</code> evaluates as <code>&quot;forty-two&quot;</code>.</p>
        <p><b>NOTE:</b> this operator does not behave like the ternary conditional
          since it does not coerce the first argument to a boolean to evaluate the
          condition. When <code>var x = false</code> and <code>var y = 0</code>,<code>x??true</code> evaluates
          as <code>false</code> and <code>y??1</code> evaluates as <code>0</code>.</p>
        <p>Note that this operator can not be overloaded</p>
      </td>
    </tr>
    <tr>
      <td style="text-align:left">Equality</td>
      <td style="text-align:left">
        <p>The usual <code>==</code> operator can be used as well as the abbreviation <code>eq</code>.
          For example <code>val1 == val2</code> and <code>val1 eq val2</code> are equivalent.</p>
        <ol>
          <li><code>null</code> is only ever equal to null, that is if you compare null
            to any non-null value, the result is false.</li>
          <li>Equality uses the java <code>equals</code> method</li>
        </ol>
      </td>
    </tr>
    <tr>
      <td style="text-align:left">In or Match<code>=~</code>
      </td>
      <td style="text-align:left">The syntactically Perl inspired <code>=~</code> operator can be used to
        check that a <code>string</code> matches a regular expression (expressed
        either a Java String or a java.util.regex.Pattern). For example <code>&quot;abcdef&quot; =~ &quot;abc.*</code> returns <code>true</code>.
        It also checks whether any collection, set or map (on keys) contains a
        value or not; in that case, it behaves as an &quot;in&quot; operator. Note
        that arrays and user classes exposing a public &apos;contains&apos; method
        will allow their instances to behave as right-hand side operands of this
        operator. <code>&quot;a&quot; =~ [&quot;a&quot;,&quot;b&quot;,&quot;c&quot;,&quot;d&quot;,&quot;e&quot;,f&quot;]</code> returns <code>true</code>.</td>
    </tr>
    <tr>
      <td style="text-align:left">Not-In or Not-Match<code>!~</code>
      </td>
      <td style="text-align:left">The syntactically Perl inspired <code>!~</code> operator can be used to
        check that a <code>string</code> does not match a regular expression (expressed
        either a Java String or a java.util.regex.Pattern). For example <code>&quot;abcdef&quot; !~ &quot;abc.*</code> returns <code>false</code>.
        It also checks whether any collection, set or map (on keys) does not contain
        a value; in that case, it behaves as &quot;not in&quot; operator. Note
        that arrays and user classes exposing a public &apos;contains&apos; method
        will allow their instances to behave as right-hand side operands of this
        operator. <code>&quot;a&quot; !~ [&quot;a&quot;,&quot;b&quot;,&quot;c&quot;,&quot;d&quot;,&quot;e&quot;,f&quot;]</code> returns <code>true</code>.</td>
    </tr>
    <tr>
      <td style="text-align:left">Starts With<code>=^</code>
      </td>
      <td style="text-align:left">The <code>=^</code> operator is a short-hand for the &apos;startsWith&apos;
        method. For example, <code>&quot;abcdef&quot; =^ &quot;abc&quot; </code>returns <code>true</code>.
        Note that through duck-typing, user classes exposing a public &apos;startsWith&apos;
        method will allow their instances to behave as left-hand side operands
        of this operator.</td>
    </tr>
    <tr>
      <td style="text-align:left">Not Starts With<code>!^</code>
      </td>
      <td style="text-align:left">This is the negation of the &apos;starts with&apos; operator. <code>a !^ &quot;abc&quot;</code> is
        equivalent to <code>!(a =^ &quot;abc&quot;)</code>
      </td>
    </tr>
    <tr>
      <td style="text-align:left">Ends With<code>=$</code>
      </td>
      <td style="text-align:left">The <code>=$</code> operator is a short-hand for the &apos;endsWith&apos;
        method. For example, <code>&quot;abcdef&quot; =$ &quot;def&quot; </code>returns <code>true</code>.
        Note that through duck-typing, user classes exposing an &apos;endsWith&apos;
        method will allow their instances to behave as left-hand side operands
        of this operator.</td>
    </tr>
    <tr>
      <td style="text-align:left">Not Ends With<code>!$</code>
      </td>
      <td style="text-align:left">This is the negation of the &apos;ends with&apos; operator. <code>a !$ &quot;abc&quot;</code> is
        equivalent to <code>!(a =$ &quot;abc&quot;)</code>
      </td>
    </tr>
  </tbody>
</table>

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

* `sampleLogKey.getDriverId()=="abcde12345"`
* `sampleLogKey.getVehicleType()=="BIKE"`
* `sampleLogKey.getEventTimestamp().getSeconds()==186178`
* `sampleLogKey.getDriverId()=="abcde12345"&&sampleLogKey.getVehicleType=="BIKE"` \(multiple conditions example 1\)
* `sampleLogKey.getVehicleType()=="BIKE"||sampleLogKey.getEventTimestamp().getSeconds()==186178` \(multiple conditions example 2\)

_**Message -based filter expressions examples:**_

* `sampleLogMessage.getGcmKey()=="abc123"`
* `sampleLogMessage.getDriverId()=="abcde12345"&&sampleLogMessage.getDriverLocation().getLatitude()>0.6487193703651428`
* `sampleLogMessage.getDriverLocation().getAltitudeInMeters>0.9949166178703308`

_**Note: Use `log` sink for testing the applied filtering**_

