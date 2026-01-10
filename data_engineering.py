import polars as pl
pl.Config.set_fmt_str_lengths(150)

# Namespaces
ns = "http://data.veronahe.no/"

# PLANETS AND SATELLITES: https://github.com/devstronomy/nasa-data-scraper/tree/master

def planets():

    # Read planet CSV
    df_planets = pl.read_csv("data/planets.csv")

    # Create subject IRI for planets
    df_planets = df_planets.with_columns(
        (ns + pl.col("planet")).alias("planet_iri")
        )

    # Chose columns to play with
    df_planets = df_planets.select(
        ["planet", 
        "planet_iri", # the new column we just made :-)
        "mean_temperature",
        "length_of_day",
        "orbital_period"
        ])
    
    return df_planets


def satellites():
    
    df_satellites = pl.read_csv("data/satellites.csv")
    df_satellites = df_satellites.with_columns(
        (ns + pl.col("planet")).alias("planet_iri")
    )

    # ensure that satellite IRI follow the IRI pattern
    # characters as / and whitespace needs to be replaced
    df_satellites = df_satellites.with_columns(
        (ns + pl.col("name")
        .str.replace_all("/", "-"))
        .str.replace_all(" ", "-")
        .alias("satellite_iri")
    )
    df_satellites = df_satellites.select(
        ["name", "planet_iri", "satellite_iri", "albedo", "radius"]
    )

    df_satellites = (df_satellites
    .with_columns(
        pl.col("albedo")
        .cast(pl.Float64, strict=False)     
        .alias("albedo")
        )
    .with_columns(
        pl.col("radius")
        .cast(pl.Float64, strict=False)     
        .alias("radius")
        )
    )
    
    return df_satellites