---
title: "Field and type options"
linkTitle: "Field and type options"
weight: 1
description: >
    Available field types and options.
---


Redis Stack provides various field types that allow you to store and search different kinds of data in your indexes. This documentation page explains the different available field types, their characteristics, and how they can be used effectively.

## Number Fields

Number fields are used to store non-textual, countable values. They can hold integer or floating-point values. Number fields are sortable, meaning you can perform range-based queries and retrieve documents based on specific numeric conditions. For example, you can search for documents with a price between a certain range or retrieve documents with a specific rating value.

You can add number fields to the schema in FT.CREATE using this syntax:

```
FT.CREATE ... SCHEMA ... {field_name} NUMBER [SORTABLE] [NOINDEX]
```

Where:

- `SORTABLE` indicates that the field can be sorted. This is useful for performing range queries and sorting search results based on numeric values.
- `NOINDEX` indicates that the field is not indexed. This is useful for storing numeric values that you don't want to search for, but you still want to retrieve them in search results.

You can search for documents with specific numeric values using the `@<field_name>:[<min> <max>]` query syntax. For example, this query finds documents with a price between 10 and 20:

```
FT.SEARCH products "@price:[200 300]"
```

You can also use the following query syntax to perform more complex numeric queries:  


| **Comparison operator** | **Query string**   | **Comment**           |
|-------------------------|--------------------|-----------------------|
| min &lt;= x &lt;= max   | @field:[min max]   | Fully inclusive range |
| min &lt; x &lt; max     | @field:[(min (max] | Fully exclusive range |
| x >= min                | @field:[min +inf]  | Upper open range      |
| x &lt;= max             | @field:[-inf max]  | Lower open range      |
| x == val                | @field:[val val]   | Equal                 |
| x != val                | -@field:[val val]  | Not equal             |


## Geo Fields

Geo fields are used to store geographical coordinates such as longitude and latitude. They enable geospatial radius queries, allowing you to find documents within a specific distance from a given point. With the support for geo fields, you can implement location-based search functionality in your applications, such as finding nearby restaurants, stores, or any other points of interest.

You can add geo fields to the schema in FT.CREATE using this syntax:

```
FT.CREATE ... SCHEMA ... {field_name} GEO [SORTABLE] [NOINDEX]
```

Where:
- `SORTABLE` indicates that the field can be sorted. This is useful for performing range queries and sorting search results based on coordinates.
- `NOINDEX` indicates that the field is not indexed. This is useful for storing coordinates that you don't want to search for, but you still want to retrieve them in search results.

You can query geo fields using the `@<field_name>:[<lon> <lat> <radius> <unit>]` query syntax. For example, this query finds documents within 1000 kilometers from the point `2.34, 48.86`:

```
FT.SEARCH cities "@coords:[2.34 48.86 1000 km]"
```

## Vector Fields

Vector fields are floating-point vectors that are typically generated by external machine learning models. These vectors represent unstructured data such as text, images, or other complex features. Redis Stack allows you to search for similar vectors using vector similarity search algorithms like cosine similarity, Euclidean distance and inner product. This enables you to build advanced search applications, recommendation systems, or content similarity analysis.

You can add vector fields to the schema in FT.CREATE using this syntax:

```
FT.CREATE ... SCHEMA ... {field_name} VECTOR {algorithm} {count} [{attribute_name} {attribute_value} ...]
```

Where:

* `{algorithm}` must be specified and be a supported vector similarity index algorithm. The supported algorithms currently are:

    - FLAT - Brute force algorithm.
    - HNSW - Hierarchical Navigable Small World algorithm.

    The `{algorithm}` attribute specifies the algorithm to use when searching k most similar vectors in the index or filtering vectors by range.

* `{count}` specifies the number of attributes for the index and it must be specified. 
Notice that `{count}` counts the total number of attributes passed for the index in the command, although algorithm parameters should be submitted as named arguments. 

    For example:

    ```
    FT.CREATE my_idx SCHEMA vec_field VECTOR FLAT 6 TYPE FLOAT32 DIM 128 DISTANCE_METRIC L2
    ```

    Here, three parameters are passed for the index (`TYPE`, `DIM`, `DISTANCE_METRIC`), and `count` counts the total number of attributes (6).

* `{attribute_name} {attribute_value}` are algorithm attributes for the creation of the vector index. Every algorithm has its own mandatory and optional attributes.

For more information about vector fields, see [Vector Fields](/docs/interact/search-and-query/search/vectors).

## Tag Fields

Tag fields in are used to store textual data that represents a collection of data tags or labels. Tag fields are characterized by their low cardinality, meaning they typically have a limited number of distinct values. Unlike text fields, tag fields are stored as-is without tokenization or stemming. They are useful for organizing and categorizing data, making it easier to filter and retrieve documents based on specific tags.

Tag fields can be added to the schema with the following syntax:

```
FT.CREATE ... SCHEMA ... {field_name} TAG [SEPARATOR {sep}] [CASESENSITIVE]
```

Where:

- `SEPARATOR` defaults to a comma (`,`), and can be any printable ASCII character. It is used to separate tags in the field value. For example, if the field value is `hello,world`, the tags are `hello` and `world`.

- `CASESENSITIVE` indicates that the field is case-sensitive. By default, tag fields are case-insensitive.

You can search for documents with specific tags using the `@<field_name>:{<tag>}` query syntax. For example, this query finds documents with the tag `blue`:

```
FT.SEARCH idx "@tags:{blue}"
```

For more information about tag fields, see [Tag Fields](/docs/interact/search-and-query/advanced-concepts/tags/).

## Text Fields

Text fields are specifically designed for storing human language text. When indexing text fields, Redis Stack performs several transformations to optimize search capabilities. The text is transformed to lowercase, allowing case-insensitive searches. The data is tokenized, meaning it is split into individual words or tokens, which enables efficient full-text search functionality. Text fields can be weighted to assign different levels of importance to specific fields during search operations. Additionally, text fields can be sorted based on their values, enabling sorting search results by relevance or other criteria.

Text fields can be added to the schema with the following syntax:

```
FT.CREATE ... SCHEMA ... {field_name} TEXT [WEIGHT] [NOSTEM] [PHONETIC {matcher}] [SORTABLE] [NOINDEX] [WITHSUFFIXTRIE]
```

Where:
- `WEIGHT` indicates that the field is weighted. This is useful for assigning different levels of importance to specific fields during search operations.
- `NOSTEM` indicates that the field is not stemmed. This is useful for storing text that you don't want to be tokenized, such as URLs or email addresses.
- `PHONETIC` Declaring a text attribute as `PHONETIC` will perform phonetic matching on it in searches by default. The obligatory {matcher} argument specifies the phonetic algorithm and language used. The following matchers are supported:

   - `dm:en` - Double metaphone for English
   - `dm:fr` - Double metaphone for French
   - `dm:pt` - Double metaphone for Portuguese
   - `dm:es` - Double metaphone for Spanish

    For more information, see [Phonetic Matching](/docs/interact/search-and-query/advanced-concepts/phonetic_matching/).
- `SORTABLE` indicates that the field can be sorted. This is useful for performing range queries and sorting search results based on text values.
- `NOINDEX` indicates that the field is not indexed. This is useful for storing text that you don't want to search for, but you still want to retrieve it in search results.
- `WITHSUFFIXTRIE` indicates that the field will be indexed with a suffix trie. The index will keep a suffix trie with all terms which match the suffix. It is used to optimize `contains (*foo*)` and `suffix (*foo)` queries. Otherwise, a brute-force search on the trie is performed. If suffix trie exists for some fields, these queries will be disabled for other fields.

You can search for documents with specific text values using the `<term>` or the `@<field_name>:{<term>}` query syntax. Let's look at a few examples:


- Search for a term in every text attribute:
    ```
    FT.SEARCH books-idx "wizard"
    ```

- Search for a term only in the `title` attribute
    ```
    FT.SEARCH books-idx "@title:dogs"
    ```