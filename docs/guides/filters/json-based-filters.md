# JSON-based Filters

To enable JSON-based filtering, you need to set the Firehose environment variable `FILTER_ENGINE=JSON` and provide the required JSON Schema filter rule to the variable`FILTER_JSON_SCHEMA .`

## JSON Schema Syntax

### Constant values

The const keyword is used to restrict a value to a single value. For example, to if you only support shipping to the United States for export reasons:

```text
{
  "properties": {
    "country": {
      "const": "United States of America"
    }
  }
}

/* valid */
{ "country": "United States of America" }

/* invalid */
{ "country": "Canada" }
```

It should be noted that const is merely syntactic sugar for an enum with a single element, therefore the following are equivalent:

```text
{ "const": "United States of America" }
{ "enum": [ "United States of America" ] }
```

### Enumerated values 

The enum keyword is used to restrict a value to a fixed set of values. It must be an array with at least one element, where each element is unique.

The following is an example for validating street light colors:

```text
{
  "type": "string",
  "enum": ["red", "amber", "green"]
}

/* valid */
"red"

/* invalid */
"blue"



```

### Numeric **Range** 

Ranges of numbers are specified using a combination of the minimum and maximum keywords, \(or exclusiveMinimum and exclusiveMaximum for expressing exclusive range\).

If x is the value being validated, the following must hold true:  
 x ≥ minimum  
 x &gt; exclusiveMinimum  
 x ≤ maximum  
 x &lt; exclusiveMaximum

Example:

```text
{
  "type": "number",
  "minimum": 0,
  "maximum": 100,
  "exclusiveMaximum": true
}


/* valid */

//exclusiveMinimum was not specified, so 0 is included:
0
10
99


/* invalid */

// Less than minimum:
-1

// exclusiveMaximum is true, so 100 is not included:
100

// Greater than maximum:
101
```

### Regex Match

The pattern keyword is used to restrict a string to a particular regular expression. The regular expression syntax is the one defined in JavaScript \(ECMA 262 specifically\). See Regular Expressions for more information.

Example:

```text
{
   "type": "string",
   "pattern": "^(\\([0-9]{3}\\))?[0-9]{3}-[0-9]{4}$"
}

// valid
"555-1212"
"(888)555-1212"

// invalid
"(888)555-1212 ext. 532"
"(800)FLOWERS"
```

### Conditional operators

The if, then and else keywords allow the application of a sub-schema based on the outcome of another schema, much like the if/then/else constructs you’ve probably seen in traditional programming languages. If if is valid, then must also be valid \(and else is ignored.\) If if is invalid, else must also be valid \(and then is ignored\).

For example, let’s say you wanted to write a schema to handle addresses in the United States and Canada. These countries have different postal code formats, and we want to select which format to validate against based on the country. If the address is in the United States, the postal\_code field is a “zipcode”: five numeric digits followed by an optional four digit suffix. If the address is in Canada, the postal\_code field is a six digit alphanumeric string where letters and numbers alternate.

```text
{
  "type": "object",
  "properties": {
    "street_address": {
      "type": "string"
    },
    "country": {
      "enum": ["United States of America", "Canada"]
    }
  },
  "if": {
    "properties": { "country": { "const": "United States of America" } }
  },
  "then": {
    "properties": { "postal_code": { "pattern": "[0-9]{5}(-[0-9]{4})?" } }
  },
  "else": {
    "properties": { "postal_code": { "pattern": "[A-Z][0-9][A-Z] [0-9][A-Z][0-9]" } }
  }
}

/* valid */
{
  "street_address": "1600 Pennsylvania Avenue NW",
  "country": "United States of America",
  "postal_code": "20500"
}

{
  "street_address": "24 Sussex Drive",
  "country": "Canada",
  "postal_code": "K1M 1M4"
}

/* invalid */
{
  "street_address": "24 Sussex Drive",
  "country": "Canada",
  "postal_code": "10000"
}
```

### Logical operators

The keywords used to combine schemas are:

* allOf: Must be valid against all of the subschemas
* oneOf: Must be valid against exactly one of the subschemas
* anyOf: Must be valid against any of the subschemas

#### allOf 

To enable JEXL-based filtering, you need to set the Firehose environment variable `FILTER_ENGINE=JEXL` and provide the required JEXL filter expression to the variable`FILTER_JEXL_EXPRESSION .`To validate against allOf, the given data must be valid against all of the given sub-schemas.

```text
{
  "allOf": [
    { "type": "string" },
    { "maxLength": 5 }
  ]
}

/* valid */
"short"

/* invalid */
"too long"


```

#### anyOf 

To validate against anyOf, the given data must be valid against any \(one or more\) of the given sub-schemas.

```text
{
  "anyOf": [
    { "type": "string", "maxLength": 5 },
    { "type": "number", "minimum": 0 }
  ]
}

/* valid */
"short"
12

/* invalid */
"too long"
-5
```

#### oneOf 

To validate against oneOf, the given data must be valid against exactly one of the given sub-schemas.

```text
{
  "oneOf": [
    { "type": "number", "multipleOf": 5 },
    { "type": "number", "multipleOf": 3 }
  ]
}

/* valid */
10
9


/* invalid */

// Not a multiple of either 5 or 3.
2

// Multiple of both 5 and 3 is rejected.
15
```

#### not 

This doesn’t strictly combine schemas, but it belongs in this chapter along with other things that help to modify the effect of schemas in some way. The not keyword declares that a instance validates if it doesn’t validate against the given sub-schema.  
  
 For example, the following schema validates against anything that is not a string:

```text
{ "not": { "type": "string" } }

/* valid */
10
9

/* invalid */
"uyu"
```

