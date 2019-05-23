library(data.table)
library(dplyr, exclude = c("between", "first", "last"))
library(fst)
library(glue, exclude = c("collapse"))
library(RPostgres)

con <- dbConnect(
  Postgres(),
  user = "postgres",
  password = "postgres",
  host = "localhost",
  port = 5433
)

try_file <- "~/Projects/try-raw-data/4143.fst"
all_try <- read_fst(try_file, as.data.table = TRUE)

# Assign a reference ID
setnames(all_try, "Reference", "Citation")
all_try <- all_try[, CitationID := .GRP, by = Citation]

# Unique tables
species_raw <- all_try[, .N, by = .(AccSpeciesID, AccSpeciesName, SpeciesName)]
datasets <- all_try[, .N, by = .(DatasetID, Dataset)]
datatypes <- all_try[, .N, by = .(TraitID, TraitName, DataID, DataName)]
citations <- all_try[, .N, by = .(CitationID, Citation)]

try_data <- all_try %>%
  select(-AccSpeciesName, -SpeciesName, -Dataset,
         -TraitName, -DataName,
         -Citation, -V28,
         -LastName, -FirstName)

# Uniqify species
dup_species <- species_raw %>%
  count(AccSpeciesID) %>%
  filter(n > 1) %>%
  inner_join(species, by = "AccSpeciesID") %>%
  group_by(AccSpeciesID, AccSpeciesName) %>%
  summarize(Synonyms = paste(SpeciesName, collapse = "|"),
            N = sum(N))

species <- species_raw %>%
  anti_join(dup_species, by = "AccSpeciesID") %>%
  rename(Synonyms = SpeciesName) %>%
  bind_rows(dup_species) %>%
  as_tibble()

dbCreateTable(con, "species", species)
dbCreateTable(con, "datasets", datasets)
dbCreateTable(con, "datatypes", datatypes)
dbCreateTable(con, "citations", citations)
dbCreateTable(con, "trydata", try_data)

set_primary_key <- function(con, table, column) {
  query <- glue::glue('ALTER TABLE "{table}" ADD PRIMARY KEY ("{column}")')
  DBI::dbExecute(con, query)
}

set_foreign_key <- function(con, table, key, foreign_table,
                            foreign_key = key,
                            constraint = paste(table, key,
                                               "fk", sep = "_")) {
  query <- glue::glue_sql(
    "ALTER TABLE {`table`} ",
    "ADD CONSTRAINT {`constraint`} ",
    "FOREIGN KEY ({`key`}) ",
    "REFERENCES {`foreign_table`} ({`foreign_key`})",
    .con = con
  )
  for (q in query) DBI::dbExecute(con, q)
}

set_primary_key(con, "species", "AccSpeciesID")
set_primary_key(con, "datasets", "DatasetID")
set_primary_key(con, "datatypes", "DataID")
set_primary_key(con, "citations", "CitationID")
set_primary_key(con, "trydata", "ObsDataID")

set_foreign_key(con, "trydata", "AccSpeciesID", "species")
set_foreign_key(con, "trydata", "DatasetID", "datasets")
set_foreign_key(con, "trydata", "DataID", "datatypes")
set_foreign_key(con, "trydata", "CitationID", "citations")

dbWriteTable(con, "species", species, append = TRUE)
dbWriteTable(con, "datasets", datasets, append = TRUE)
dbWriteTable(con, "datatypes", datatypes, append = TRUE)
dbWriteTable(con, "citations", citations, append = TRUE)

remove_invalid_utf8 <- function(x) {
  if (!is.character(x)) return(x)
  Encoding(x) <- "UTF-8"
  iconv(x, "UTF-8", "UTF-8", sub = "")
}

try_data_fixed <- try_data[, lapply(.SD, remove_invalid_utf8)]

nsplit <- 100
splitby <- rep(seq.int(1, nsplit), length.out = nrow(try_data_fixed))
try_data_split <- split(try_data_fixed, splitby)

pb <- progress::progress_bar$new(total = nsplit)
for (i in seq_along(try_data_split)) {
  pb$tick()
  dbWriteTable(con, "trydata", try_data_split[[i]],
               copy = TRUE, append = TRUE)
}
