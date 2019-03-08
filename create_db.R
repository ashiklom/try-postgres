requireNamespace("data.table", quietly = TRUE)
# begin imports
import::from("magrittr", "%>%", .into = "")
import::from("DBI", "dbConnect", "dbCreateTable", "dbSendQuery", "dbRemoveTable", "dbAppendTable", .into = "")
import::from("RPostgres", "Postgres", .into = "")
import::from("dplyr", "distinct", "select", "tbl", "count", "group_by", "mutate", "filter", "collect", .into = "")
import::from("fst", "read_fst", .into = "")
import::from("glue", "glue", .into = "")
# end imports
import::from("data.table", "fwrite", .into = "")

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
data.table::setnames(all_try, "Reference", "Citation")
all_try <- all_try[, CitationID := .GRP, by = Citation]

# Unique tables
species <- all_try[, .N, by = .(AccSpeciesID, AccSpeciesName, SpeciesName)]
datasets <- all_try[, .N, by = .(DatasetID, Dataset)]
datatypes <- all_try[, .N, by = .(TraitID, TraitName, DataID, DataName)]
citations <- all_try[, .N, by = .(CitationID, Citation)]

try_data <- all_try %>%
  select(-AccSpeciesName, -SpeciesName, -Dataset,
         -TraitName, -DataName,
         -Citation, -V28,
         -LastName, -FirstName)

dbCreateTable(con, "species", species)
dbCreateTable(con, "datasets", datasets)
dbCreateTable(con, "datatypes", datatypes)
dbCreateTable(con, "citations", citations)
dbCreateTable(con, "trydata", try_data)

dup_species <- tbl(con, "species") %>%
  count(AccSpeciesID) %>%
  filter(n > 1) %>%
  collect()

set_primary_key <- function(con, table, column) {
  query <- glue::glue('ALTER TABLE "{table}" ADD PRIMARY KEY ("{column}")')
  dbSendQuery(con, query)
}

set_primary_key(con, "species", "AccSpeciesID")
set_primary_key(con, "species", "AccSpeciesID")

dbSendQuery(con, 'ALTER TABLE species ADD PRIMARY KEY ("AccSpeciesID")')
dbSendQuery(con, 'ALTER TABLE species ADD PRIMARY KEY ("AccSpeciesID")')
dbSendQuery(con, paste(
  "ALTER TABLE trydata ",
  "ADD CONSTRAINT species_fk ",
  'FOREIGN KEY ("AccSpeciesID") REFERENCES species ("AccSpeciesID")'
))
dbSendQuery(con, paste(
  "ALTER TABLE trydata",
  "ADD CONSTRAINT dataset_fk",
  'FOREIGN KEY ("DatasetID") REFERENCES datasets ("DatasetID")'
))
dbSendQuery(con, paste(
  "ALTER TABLE trydata",
  "ADD CONSTRAINT trait_fk",
  "FOREIGN KEY (TraitID, DataID) REFERENCES datatypes (TraitID, DataID)"
))
dbSendQuery(con, paste(
  "ALTER TABLE trydata",
  "ADD CONSTRAINT reference_fk",
  "FOREIGN KEY (CitationID) REFERENCES citations (CitationID)"
))
dbSendQuery(con, paste(
  "ALTER TABLE trydata",
  "ADD PRIMARY KEY (\"ObsDataID\")"
))

dbAppendTable(con, "species", species)
dbAppendTable(con, "datasets", datasets)
dbAppendTable(con, "datatypes", datatypes)
dbAppendTable(con, "citations", citations)

temp_file <- tempfile(fileext = ".csv")
fwrite(try_data, temp_file)
dbSendQuery(con, glue(
  "COPY trydata FROM {temp_file}"
))
dbAppendTable(con, "trydata", try_data)

count(try_data)
tbl(con, "trydata")


if (FALSE) {
  dbRemoveTable(con, "species")
  dbRemoveTable(con, "datasets")
  dbRemoveTable(con, "datatypes")
  dbRemoveTable(con, "citations")
  dbRemoveTable(con, "trydata")
}
