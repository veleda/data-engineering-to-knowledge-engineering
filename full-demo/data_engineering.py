import polars as pl
pl.Config.set_fmt_str_lengths(150)

# Namespaces
ns = "http://data.treehouse.example/"
ns_tpl = "http://data.treehouse.example/tpl/" # for OTTR templates
ns_sh = "http://data.treehouse.example/sh/" # for SHACL shapes

# DATA SOURCES:
# - Customers:       Parquet   (close to Databricks / lakehouse patterns)
# - Orders:          CSV       (classic flat-file exchange format)
# - Products:        Excel     (typical business-user data source)
# - Support Tickets: SQL       (using Polars SQLContext as a lightweight SQL endpoint)
#
# ENTITY HARMONIZATION:
# In real organisations, the same entity (e.g. a customer) appears across
# multiple source systems — each with its own naming conventions:
#
#   customers.parquet  →  "customer_id", "full_name", "registered_at"
#   orders.csv         →  "cust_ref",    (no name),   "order_date"
#   products.xlsx      →  "sku",         "name",      "price"
#   support_tickets    →  "client_id",   (no name),   "date_created"
#
# The customer foreign key alone has THREE different names: customer_id,
# cust_ref, and client_id. The "name" column means the customer's name in
# the Parquet file (called "full_name") but the product's name in Excel
# (just "name"). Dates are "registered_at", "order_date", "date_created".
#
# A knowledge graph handles this by assigning each entity a single IRI —
# a globally unique identity — dIRIng the data engineering step. The OTTR
# template then maps from the harmonised column names to RDF predicates.
#
# The harmonization happens HERE, in data_engineering.py. Each function:
#   1. Reads from the source in its native format and naming convention
#   2. Renames columns to a shared vocabulary (the OTTR template contract)
#   3. Constructs IRIs that link entities across sources
#
# This is the "mapping magic" — and it's the step that makes the knowledge
# graph possible. Without it, you'd have four disconnected datasets.


def customers():
    """
    Load customer master data from a Parquet file.
    Parquet is the default columnar format in Databricks / Spark lakehouse architectures.
    Reading parquet with Polars is zero-copy and very fast.

    Source columns:  customer_id, full_name, email, country, segment, registered_at
    Harmonised to:   customer_iri, name, email, country, segment, signup_date
    """

    # Read parquet — Polars handles schema inference automatically
    df_customers = pl.read_parquet("data/customers.parquet")

    # HARMONIZE: Rename source-specific column names to match our shared vocabulary.
    # The Parquet file calls it "full_name" —> our ontology just says "name".
    # The date field is "registered_at" —> we normalise to "signup_date".
    df_customers = df_customers.rename({
        "full_name": "name",               # full_name → name (OTTR template expects "name")
        "registered_at": "signup_date",     # registered_at → signup_date
    })

    # Create a IRI for each customer based on their customer_id.
    # This IRI will become the subject in the knowledge graph — the single
    # identity that ties this customer to their orders and tickets.
    df_customers = df_customers.with_columns(
        (ns + pl.col("customer_id")).alias("customer_iri")
    )

    # Select the columns we need for the OTTR template mapping.
    # At this point the column names match the OTTR template signature.
    df_customers = df_customers.select([
        "name",
        "customer_iri",       # the RDF subject IRI we just created
        "email",
        "country",
        "segment",
        "signup_date",
    ])

    return df_customers


def orders():
    """
    Load order data from a CSV file.
    CSV is the most common flat-file exchange format,
    widely used for data exports and integrations.

    Source columns:  order_id, cust_ref, prod_code, quantity, total_amount, order_date, order_status
    Harmonised to:   order_iri, customer_iri, product_iri, quantity, total_amount, order_date, status
    """

    # Read CSV — Polars infers types, but we may need to cast some columns
    df_orders = pl.read_csv("data/orders.csv")

    # HARMONIZE: The order system uses its own naming conventions.
    # "cust_ref" is their name for what the customer master calls "customer_id".
    # "prod_code" is their name for what the product catalog calls "sku".
    # "order_status" needs to become "status" to match our OTTR template.
    #
    # This is exactly the problem raised in the Practical Data Community
    # discussion: three systems, three names for the same foreign key.
    # The data engineer's job is to align them BEFORE mapping to the graph.
    df_orders = df_orders.rename({
        "cust_ref": "customer_id",         # cust_ref → customer_id (align with customer master)
        "prod_code": "product_id",         # prod_code → product_id (align with product catalog)
        "order_status": "status",          # order_status → status
    })

    # Create IRI for each order
    df_orders = df_orders.with_columns(
        (ns + pl.col("order_id")
         .str.replace_all(" ", "-"))
        .alias("order_iri")
    )

    # Create IRI for the customer this order belongs to.
    # Because we renamed "cust_ref" → "customer_id" above, this now produces
    # the SAME IRI as in the customers() function — that's how the graph links
    # orders to their customers without a SQL JOIN.
    df_orders = df_orders.with_columns(
        (ns + pl.col("customer_id")).alias("customer_iri")
    )

    # Create IRI for the product in this order.
    # The order file called it "prod_code"; the product file calls it "sku".
    # Both refer to the same entity (e.g. "P0003"). After renaming to
    # "product_id", the IRI construction produces the same IRI on both sides.
    df_orders = df_orders.with_columns(
        (ns + pl.col("product_id")).alias("product_iri")
    )

    # Ensure numeric columns have the right types for OTTR mapping
    df_orders = df_orders.with_columns(
        pl.col("quantity").cast(pl.Int64, strict=False).alias("quantity"),
        pl.col("total_amount").cast(pl.Float64, strict=False).alias("total_amount"),
    )

    # Select columns matching the OTTR template signature
    df_orders = df_orders.select([
        "order_id",
        "order_iri",          # RDF subject
        "customer_iri",       # reference to customer (was "cust_ref" in the source)
        "product_iri",        # reference to product  (was "prod_code" in the source)
        "quantity",
        "total_amount",
        "order_date",
        "status",
    ])

    return df_orders


def products():
    """
    Load product catalog from an Excel (.xlsx) file.
    Excel is a typical source for business-managed data
    like product catalogs, pricing sheets, or inventory lists.

    Source columns:  sku, name, category, price, in_stock
    Harmonised to:   product_iri, product_name, category, unit_price, stock_quantity
    """

    # Read Excel — Polars uses the calamine engine for fast xlsx reading
    df_products = pl.read_excel("data/products.xlsx")

    # HARMONIZE: The Excel file comes from the product team — they use their
    # own conventions. "sku" is their identifier (what orders call "prod_code"),
    # "name" means the product name (NOT the customer name!), "price" is short
    # for "unit_price", and "in_stock" is their "stock_quantity".
    #
    # Note the ambiguity: "name" in this file is a product name, while
    # "full_name" in customers.parquet is a person's name. Column names alone
    # don't carry semantics — the ontology does. This is why we rename to
    # explicit, qualified names before mapping to the graph.
    df_products = df_products.rename({
        "sku": "product_id",               # sku → product_id (canonical identifier)
        "name": "product_name",            # name → product_name (disambiguate from customer name)
        "price": "unit_price",             # price → unit_price
        "in_stock": "stock_quantity",      # in_stock → stock_quantity
    })

    # Create IRI for each product — using "product_id" which was "sku" in the source
    df_products = df_products.with_columns(
        (ns + pl.col("product_id")).alias("product_iri")
    )

    # Ensure price and stock are numeric
    df_products = df_products.with_columns(
        pl.col("unit_price").cast(pl.Float64, strict=False).alias("unit_price"),
        pl.col("stock_quantity").cast(pl.Int64, strict=False).alias("stock_quantity"),
    )

    # Select columns matching the OTTR template signature
    df_products = df_products.select([
        "product_name",
        "product_iri",        # RDF subject
        "category",
        "unit_price",
        "stock_quantity",
    ])

    return df_products


def support_tickets():
    """
    Load support ticket data via a SQL endpoint.
    Uses Polars SQLContext to register a DataFrame as a virtual table,
    then queries it with standard SQL — simulating a SQL endpoint / database.
    This pattern is useful when your source is a SQL database or warehouse.

    Source columns:  ticket_id, client_id, type, severity, state, date_created
    Harmonised to:   ticket_iri, customer_iri, issue_type, priority, ticket_status, created_date
    """

    # First, load the raw ticket data into a Polars DataFrame
    # (in a real scenario this could come from a database connector)
    df_raw = pl.read_csv("data/support_tickets.csv")

    # Register the DataFrame as a SQL table in the Polars SQLContext
    # This simulates connecting to a SQL endpoint (e.g. Databricks SQL, Snowflake, Postgres)
    ctx = pl.SQLContext(support_tickets=df_raw)

    # HARMONIZE inside the SQL query itself — this is a natural place to do it
    # when your source is a SQL database. The CRM system uses its own vocabulary:
    #   "client_id"    → what the customer master calls "customer_id"
    #   "type"         → what we call "issue_type"
    #   "severity"     → what we call "priority"
    #   "state"        → what we call "ticket_status" (and orders call "status")
    #   "date_created" → what we call "created_date" (word order flipped!)
    #
    # SQL aliases handle the renaming cleanly. This is one advantage of the
    # SQL source pattern: you can harmonize in the same step as filtering.
    df_tickets = ctx.execute("""
        SELECT
            ticket_id,
            client_id    AS customer_id,       -- client_id → customer_id
            type         AS issue_type,         -- type → issue_type
            severity     AS priority,           -- severity → priority
            state        AS ticket_status,      -- state → ticket_status
            date_created AS created_date        -- date_created → created_date
        FROM support_tickets
        WHERE state != 'cancelled'
        ORDER BY date_created DESC
    """).collect()

    # Create IRIs for the knowledge graph.
    # Because we aliased "client_id" → "customer_id" in the SQL above,
    # the IRI for customer_id="C0012" here will be identical to the IRI
    # created in customers() and orders() — that's the link.
    df_tickets = df_tickets.with_columns(
        (ns + pl.col("ticket_id")
         .str.replace_all(" ", "-"))
        .alias("ticket_iri")
    )

    df_tickets = df_tickets.with_columns(
        (ns + pl.col("customer_id")).alias("customer_iri")
    )

    # Select columns matching the OTTR template signature
    df_tickets = df_tickets.select([
        "ticket_id",
        "ticket_iri",         # RDF subject
        "customer_iri",       # reference to customer (was "client_id" in the source)
        "issue_type",
        "priority",
        "ticket_status",
        "created_date",
    ])

    return df_tickets


def column_mapping_table():
    """
    Return a Polars DataFrame showing how source column names are
    harmonised to a shared vocabulary before mapping to the graph.
    Useful for documentation and for the notebook walkthrough.
    """
    return pl.DataFrame({
        "source_file": [
            "customers.parquet", "customers.parquet",
            "orders.csv", "orders.csv", "orders.csv",
            "products.xlsx", "products.xlsx", "products.xlsx", "products.xlsx",
            "support_tickets.csv", "support_tickets.csv", "support_tickets.csv",
            "support_tickets.csv", "support_tickets.csv",
        ],
        "source_column": [
            "full_name", "registered_at",
            "cust_ref", "prod_code", "order_status",
            "sku", "name", "price", "in_stock",
            "client_id", "type", "severity", "state", "date_created",
        ],
        "harmonised_to": [
            "name", "signup_date",
            "customer_id", "product_id", "status",
            "product_id", "product_name", "unit_price", "stock_quantity",
            "customer_id", "issue_type", "priority", "ticket_status", "created_date",
        ],
        "why": [
            "OTTR template expects 'name'",
            "Normalise date naming",
            "Align with customer master PK",
            "Align with product catalog PK",
            "Match OTTR template field",
            "Canonical product identifier",
            "Disambiguate from customer 'full_name'",
            "Full name for clarity",
            "Match OTTR template field",
            "Align with customer master PK",
            "Qualify as issue_type",
            "Align with SKOS priority scheme",
            "Match OTTR template field",
            "Normalise date naming",
        ],
    })
