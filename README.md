# Type Domain of Wikidata Entities

This repository explains the **type domain** of Wikidata entities as defined in our work. The entity types are categorized into three main groups: **Explicit Entity Types**, **Extended Entity Types**, and **NER Entity Types**. Below is a detailed explanation of each category:

---

## Entity Type Categories

### 1. **Explicit Entity Types** (WD Types)
- These are the **types explicitly associated** with entities in Wikidata.
- They are **directly linked** to the entity through the Wikidata ontology.
- Represent **predefined categories** such as:
  - *Capital city*
  - *Human*
  - *Television series*
- These types are detailed in tables referenced in the original work (e.g., tables showing data with and without filters).

---

### 2. **Extended Entity Types**
- These types start as **explicit types** in Wikidata but are **extended through deterministic procedures**.
- **Extension Process**:
  - Includes operations such as **transitive closure**, which helps uncover indirect relationships or associations.
  - Example: From the explicit type *Capital city*, additional related types such as *Administrative centre* or *Geographic location* are identified.
- This category enhances the depth of classification by capturing **indirect associations** that are not directly stated in Wikidata.

#### Algorithm: Extending Entity Types Using Superclass Retrieval with Recursive Relationships

This algorithm describes the process of extending entity types by retrieving their superclasses from a hierarchical ontology, such as Wikidata. It utilizes the **P279 ("subclass of")** property and employs a recursive pattern to traverse the hierarchy and include indirect relationships.

---

#### Steps

#### 1. **Input Collection**
- Start with a list of **explicit types** associated with an entity, represented by their unique identifiers (e.g., `Q207784`).

---

#### 2. **Define Query**
- Construct a query to retrieve all **superclasses** of the given type(s) using the property **P279 ("subclass of")**.
- Use a **recursive pattern** to traverse the hierarchy and include all parent classes:
  ```sparql
  ?entity (wdt:P279)* ?superclass

---

#### 3. **Extend types**
- Combine the retrieved superclasses with the original explicit types.
- The final extended types list will be a set of all the lists retrieved associated to the explicit WD types of the entity


### 3. **NER Entity Types** (NER Types)
- A specialized form of **explicit type extension** that maps Wikidata types to **generic categories** based on a flat classification scheme.
- Derived from the most common categories in **Named Entity Recognition (NER)** tasks.
- The four macro-classes used are:
  1. **ORG** (Organizations)
  2. **LOC** (Locations)
  3. **PERS** (Persons)
  4. **OTHERS** (Entities outside these main categories)
- **Implementation**:
  - The mapping methodology is based on the paper **"NECKAr"** by Geiss et al. (2018).


## Algorithm Overview

1. **For each Wikidata (WD) entity**, all associated types are evaluated for extension.
2. Types are extended if they are present in one of the following lists:
   - **`organization_subclass`**
   - **`geolocation_subclass`**
   - **`person`** (limited to the ID `5` - *Q5*, representing "human").

3. **Adjustments for `organization_subclass`:**
   - Certain subclasses are **excluded** from the `organization_subclass` list, such as:
     - Country
     - Capitals
     - Venues
     - (and others as specified in the processing logic).

4. **Type Classification:**
   - For each entity, the list of mapped **NER type** among the extended types is selected as the final classification.
   - Example: If an entity has types that are all present in `geolocation_subclass`, it will be classified as **LOC (Location)**.

---

## Example

### Entity: Belgium (Q31)
- **Original Types:** Various types associated with Belgium.
- **Processing:**
  - The types are checked against the `geolocation_subclass` and `organization_subclass`.
  - Belgium (Q31) is classified as **LOC (Location)** and **ORG (Organization)**.

---

---

## Purpose of This Work
- To **categorize and expand the understanding** of entity types in Wikidata.
- To enhance **classification and retrieval processes** by leveraging both direct and extended associations.

## References
- For further information on the NER mapping approach, see: [NECKAr](https://link.springer.com/content/pdf/10.1007/978-3-319-73706-5_10.pdf) by Geiss et al.

---

Feel free to explore the code and examples in this repository for a deeper understanding of how the entity types are defined, extended, and mapped to NER categories.
