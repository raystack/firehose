# JSON-based Filters

### Numeric **Range** 

* Ranges of numbers are specified using a combination of the minimum and maximum keywords, \(or exclusiveMinimum and exclusiveMaximum for expressing exclusive range\).
* If x is the value being validated, the following must hold true:  x ≥ minimum  x &gt; exclusiveMinimum  x ≤ maximum  x &lt; exclusiveMaximum
* Example:

```text
/* ---- EXAMPLE #1 ---- */
{
  "type": "number",
  "minimum": 0,
  "exclusiveMaximum": 100
}


/* valid */

// minimum is inclusive, so 0 is valid:
0
10
99



/* invalid */

// Less than minimum:
-1

// exclusiveMaximum is exclusive, so 100 is not valid:
100

// Greater than maximum:
101










/* ---- EXAMPLE #2 ---- */
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



### Regular Expressions \([https://json-schema.org/understanding-json-schema/reference/string.html\#id6](https://json-schema.org/understanding-json-schema/reference/string.html#id6)\)

* The pattern keyword is used to restrict a string to a particular regular expression. The regular expression syntax is the one defined in JavaScript \(ECMA 262 specifically\). See Regular Expressions for more information.
* Example:

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



### Enumerated values \([https://json-schema.org/understanding-json-schema/reference/generic.html\#id4](https://json-schema.org/understanding-json-schema/reference/generic.html#id4)\)

* The enum keyword is used to restrict a value to a fixed set of values. It must be an array with at least one element, where each element is unique.

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










/* ---- EXAMPLE #2 ---- */

// You can use enum even without a type, to accept values of different types. Let’s extend the example to use null to indicate “off”, and also add 42, just for fun.
{
  "enum": ["red", "amber", "green", null, 42]
}

/* valid */
"red"
null
42

/* invalid */
0










/* ---- EXAMPLE #3 ---- */

// However, in most cases, the elements in the enum array should also be valid against the enclosing schema:
{
  "type": "string",
  "enum": ["red", "amber", "green", null]
}

/* valid */
"red"

/* invalid */
null
```

### 

### Constant values \([https://json-schema.org/understanding-json-schema/reference/generic.html\#id5](https://json-schema.org/understanding-json-schema/reference/generic.html#id5)\)

* The const keyword is used to restrict a value to a single value. For example, to if you only support shipping to the United States for export reasons:

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

## Combining schemas \([https://json-schema.org/understanding-json-schema/reference/combining.html\#combining-schemas](https://json-schema.org/understanding-json-schema/reference/combining.html#combining-schemas)\)

* JSON Schema includes a few keywords for combining schemas together. Note that this doesn’t necessarily mean combining schemas from multiple files or JSON trees, though these facilities help to enable that and are described in Structuring a complex schema. Combining schemas may be as simple as allowing a value to be validated against multiple criteria at the same time.

For example, in the following schema, the anyOf keyword is used to say that the given value may be valid against any of the given subschemas. The first subschema requires a string with maximum length 5. The second subschema requires a number with a minimum value of 0. As long as a value validates against either of these schemas, it is considered valid against the entire combined schema.

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

The keywords used to combine schemas are:

* allOf: Must be valid against all of the subschemas
* anyOf: Must be valid against any of the subschemas
* oneOf: Must be valid against exactly one of the subschemas

### 

### allOf \([https://json-schema.org/understanding-json-schema/reference/combining.html\#id5](https://json-schema.org/understanding-json-schema/reference/combining.html#id5)\)

* To validate against allOf, the given data must be valid against all of the given subschemas.

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






/* ---- EXAMPLE #2 ---- */
{
  "allOf": [
    { "type": "string" },
    { "type": "number" }
  ]
}
```

### 

### allOf \([https://json-schema.org/understanding-json-schema/reference/combining.html\#id5](https://json-schema.org/understanding-json-schema/reference/combining.html#id5)\)

* To validate against allOf, the given data must be valid against all of the given subschemas.

```text
{

/* invalid */
"No way"
-1











/* ---- EXAMPLE #3 ---- */
// It is important to note that the schemas listed in an allOf, anyOf or oneOf array know nothing of one another. While it might be surprising, allOf can not be used to “extend” a schema to add more details to it in the sense of object-oriented inheritance. For example, say you had a schema for an address in a definitions section, and want to extend it to include an address type:
{
  "definitions": {
    "address": {
      "type": "object",
      "properties": {
        "street_address": { "type": "string" },
        "city":           { "type": "string" },
        "state":          { "type": "string" }
      },
      "required": ["street_address", "city", "state"]
    }
  },

  "allOf": [
    { "$ref": "#/definitions/address" },
    { "properties": {
        "type": { "enum": [ "residential", "business" ] }
      }
    }
  ]
}

/* valid */
{
   "street_address": "1600 Pennsylvania Avenue NW",
   "city": "Washington",
   "state": "DC",
   "type": "business"
}









/* ---- EXAMPLE #4 ---- */
// This works, but what if we wanted to restrict the schema so no additional properties are allowed? One might try adding the highlighted line below:
{
  "definitions": {
    "address": {
      "type": "object",
      "properties": {
        "street_address": { "type": "string" },
        "city":           { "type": "string" },
        "state":          { "type": "string" }
      },
      "required": ["street_address", "city", "state"]
    }
  },

  "allOf": [
    { "$ref": "#/definitions/address" },
    { "properties": {
        "type": { "enum": [ "residential", "business" ] }
      }
    }
  ],

  "additionalProperties": false
}

/* invalid */
{
   "street_address": "1600 Pennsylvania Avenue NW",
   "city": "Washington",
   "state": "DC",
   "type": "business"
}
```

### 

### anyOf \([https://json-schema.org/understanding-json-schema/reference/combining.html\#anyof](https://json-schema.org/understanding-json-schema/reference/combining.html#anyof)\)

* To validate against anyOf, the given data must be valid against any \(one or more\) of the given subschemas.

```text
{
  "anyOf": [
    { "type": "string" },
    { "type": "number" }
  ]
}

/* valid */
"Yes"
42

/* invalid */
{ "Not a": "string or number" }
```

### 

### oneOf \([https://json-schema.org/understanding-json-schema/reference/combining.html\#oneof](https://json-schema.org/understanding-json-schema/reference/combining.html#oneof)\)

* To validate against oneOf, the given data must be valid against exactly one of the given subschemas.

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

### 

### not \([https://json-schema.org/understanding-json-schema/reference/combining.html\#id8](https://json-schema.org/understanding-json-schema/reference/combining.html#id8)\)

* This doesn’t strictly combine schemas, but it belongs in this chapter along with other things that help to modify the effect of schemas in some way. The not keyword declares that a instance validates if it doesn’t validate against the given subschema.   For example, the following schema validates against anything that is not a string:

```text
{ "not": { "type": "string" } }
/* valid */
10
9



/* invalid */

// Not a multiple of either 5 or 3.
42
{ "key": "value" }

// Multiple of both 5 and 3 is rejected.
"I am a string"
```

## Applying subschemas conditionally \([https://json-schema.org/understanding-json-schema/reference/conditionals.html\#applying-subschemas-conditionally](https://json-schema.org/understanding-json-schema/reference/conditionals.html#applying-subschemas-conditionally)\)

* The if, then and else keywords allow the application of a subschema based on the outcome of another schema, much like the if/then/else constructs you’ve probably seen in traditional programming languages. If if is valid, then must also be valid \(and else is ignored.\) If if is invalid, else must also be valid \(and then is ignored\).
* For example, let’s say you wanted to write a schema to handle addresses in the United States and Canada. These countries have different postal code formats, and we want to select which format to validate against based on the country. If the address is in the United States, the postal\_code field is a “zipcode”: five numeric digits followed by an optional four digit suffix. If the address is in Canada, the postal\_code field is a six digit alphanumeric string where letters and numbers alternate.

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

* Unfortunately, this approach above doesn’t scale to more than two countries. You can, however, wrap pairs of if and then inside an allOf to create something that would scale. In this example, we’ll use United States and Canadian postal codes, but also add Netherlands postal codes, which are 4 digits followed by two letters. It’s left as an exercise to the reader to expand this to the remaining postal codes of the world.

```text
{
  "type": "object",
  "properties": {
    "street_address": {
      "type": "string"
    },
    "country": {
      "enum": ["United States of America", "Canada", "Netherlands"]
    }
  },
  "allOf": [
    {
      "if": {
        "properties": { "country": { "const": "United States of America" } }
      },
      "then": {
        "properties": { "postal_code": { "pattern": "[0-9]{5}(-[0-9]{4})?" } }
      }
    },
    {
      "if": {
        "properties": { "country": { "const": "Canada" } }
      },
      "then": {
        "properties": { "postal_code": { "pattern": "[A-Z][0-9][A-Z] [0-9][A-Z][0-9]" } }
      }
    },
    {
      "if": {
        "properties": { "country": { "const": "Netherlands" } }
      },
      "then": {
        "properties": { "postal_code": { "pattern": "[0-9]{4} [A-Z]{2}" } }
      }
    }
  ]
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

{
  "street_address": "Adriaan Goekooplaan",
  "country": "Netherlands",
  "postal_code": "2517 JX"
}


/* invalid */
{
  "street_address": "24 Sussex Drive",
  "country": "Canada",
  "postal_code": "10000"
}
```

