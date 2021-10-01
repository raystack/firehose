# JSON-based Filters

To enable JSON-based filtering, you need to set the Firehose environment variable as`FILTER_ENGINE=JSON` and provide the required JSON Schema filter rule to the variable`FILTER_JSON_SCHEMA`. JSON-based filters can be applied to both JSON and Protobuf messages. 

This article enlists only a few common features of JSON Schema. For more details on other features, refer the [JSON Schema specifications](https://json-schema.org/specification.html). 

## JSON Schema Features

### Checking equality

The `const` keyword is used to restrict a value to a single value. 

#### String

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

#### Integer

```text
{
  "properties": {
    "age": {
      "const": 23
    }
  }
}

/* valid */
{ "age": 23 }

/* invalid */
{ "age": "23" }
{ "age": 29 }
```

#### Float

```text
{
  "properties": {
    "price": {
      "const": 932.4556
    }
  }
}

/* valid */
{ "price": 932.4556 }

/* invalid */
{ "price": "932.4556" }
{ "price": 932.455 }
```

#### Boolean

```text
{
  "properties": {
    "auth_enabled": {
      "const": true
    }
  }
}

/* valid */
{ "auth_enabled": true }

/* invalid */
{ "auth_enabled": "true" }
{ "auth_enabled": false }
```

#### Array

```text
{
  "properties": {
    "roll_nos": {
      "const": [5,23,7]
    }
  }
}

/* valid */
{ "roll_nos": [5,23,7] }

/* invalid */
{ "roll_nos": [23,2,7] }
```

### Enumerated values

The `enum` keyword is used to restrict a value to a fixed set of values. It must be an array with at least one element, where each element is unique.

The following is an example for validating street light colors:

```text
{
   "properties":{
      "color":{
         "enum":[ "red","amber", "green" ]
      }
   }
}

/* valid */
{"color":"red"}

/* invalid */
{"color":"blue"}
```

### Numeric **Range**

Ranges of numbers are specified using a combination of the `minimum` and `maximum` keywords, \(or `exclusiveMinimum` and `exclusiveMaximum` for expressing exclusive range\).

If x is the value being validated, the following must hold true:  
x ≥ `minimum`  
x &gt; `exclusiveMinimum`  
x ≤ `maximum`  
x &lt; `exclusiveMaximum`

Example:

```text
{
   "properties":{
      "age":{
         "minimum":0,
         "maximum":100
      }
   }
}


/* valid */
{"age":0}
{"age":100}
{"age":99}


/* invalid */
{"age":-1}
{"age":101}
```

### Regex Match

The `pattern` keyword is used to restrict a string to a particular regular expression. The regular expression syntax is the one defined in JavaScript \(ECMA 262 specifically\). See Regular Expressions for more information.

Example:

```text
{
   "properties":{
      "pincode":{
         "pattern":"^(\\([0-9]{3}\\))?[0-9]{3}-[0-9]{4}$"
      }
   }
}

// valid
{ "pincode": "555-1212" }
{ "pincode": "(888)555-1212" }

// invalid
{ "pincode": "(888)555-1212 ext. 532" }
{ "pincode": "(800)FLOWERS" }
```

### Conditional operators

The **`if`, `then` and `else`** keywords allow the application of a sub-schema based on the outcome of another schema, much like the if/then/else constructs you’ve probably seen in traditional programming languages. If if is valid, then must also be valid \(and else is ignored.\) If if is invalid, else must also be valid \(and then is ignored\).

```text
{
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

* `allOf`: Must be valid against all of the sub-schemas
* `oneOf`: Must be valid against exactly one of the sub-schemas
* `anyOf`: Must be valid against any of the sub-schemas

#### allOf

To validate against `allOf`, the given data must be valid against all of the given sub-schemas.

```text
{
   "properties":{
      "age":{
         "allOf":[
            { "multipleOf":5 },
            { "multipleOf":3 }
         ]
      }
   }
}

/* valid */
{"age:15}
{"age:30}


/* invalid */
{"age:5}
{"age:9}
```

#### anyOf

To validate against `anyOf`, the given data must be valid against any \(one or more\) of the given sub-schemas.

```text
{
   "properties":{
      "age":{
         "anyOf":[
            { "multipleOf":5 },
            { "multipleOf":3 }
         ]
      }
   }
}

/* valid */
{"age:10}
{"age:15}


/* invalid */
{"age:2}
{"age:7}
```

#### oneOf

To validate against `oneOf`, the given data must be valid against exactly one of the given sub-schemas.

```text
{
   "properties":{
      "age":{
         "oneOf":[
            { "multipleOf":5 },
            { "multipleOf":3 }
         ]
      }
   }
}

/* valid */
{"age:10}
{"age:9}


/* invalid */
{"age:2}
{"age:15}
```

#### not

The `not` keyword declares that a instance validates if it doesn’t validate against the given sub-schema.

```text
{
   "properties":{
      "fruit":{
         "not":{
            "const":"apple"
         }
      }
   }
}

/* valid */
{"fruit":"mango"}
{"fruit":"errr"}

/* invalid */
{"fruit":"apple"}
```

### Nested fields

You can apply all the above validation features to any level of nested fields in the JSON/ Protobuf message. Consider the below example - 

```text
{
   "properties":{
      "driver_location":{
         "properties":{
            "latitude":{
               "minimum":-90.453,
               "maximum":90.2167
            },
            "longitude":{
               "minimum":-180.776,
               "maximum":180.321
            }
         }
      }
   }
}
```

