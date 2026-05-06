### Autor: Michael Strohbach
### Datum: 29.04.2025
### Zweck: Biodiversitätsdatenmanagement Übung 5 (Datenbanken) 


######################
#### 1. Erstellen einer Datenbank aus externen Daten
#####################

library(duckdb)
# falls noch nicht installiert install.packages("duckdb")

# der Pfad muss so angepasst werden, dass die Dateien parks.csv und species.csv im Arbeitsverzeichnis liegen. Wenn das Skript Teil eines R-Studio-Projekts ist, sollte es funktionieren.  

# wir erstellen uns eine Datenbank-Datei 
con <- dbConnect(duckdb(), dbdir = "arten_NP.duckdb", read_only = FALSE)
# con <- dbConnect(duckdb(), dbdir = "arten_NP.duckdb") wenn DB schon existiert


# Wir laden die CSV-Dateine in die DuckDatenbank. Es handelt sich um Artenerfassungen in US Amerikanischen Nationalparks, die ich von Kaggle runter geladen hab https://www.kaggle.com/datasets/nationalparkservice/park-biodiversity
# wir bekommen die Daten mit einem SQL-Befehl in die Datenbank. SQL ist recht nah an der Sprache, hier gibt es viele Beispiele: https://duckdb.org/docs/stable/sql/introduction. Mit dbExecute führen wir Befehle aus, die die Datenbank verändern. 
dbExecute(con, "CREATE TABLE parks AS SELECT * FROM 'parks.csv'")
dbExecute(con, "CREATE TABLE species AS SELECT * FROM 'species.csv'")


### Ok, was haben wir da? 
dbGetInfo(con)
dbListObjects(con) # wir haben eine Datenbank mit mehreren Tabellen


### jetzt können wir uns ein paar Sachen anzeigen. Mit dbGetQuery führen wir Abfragen aus - die verändern die Datenbank nicht. 
dbGetQuery(con, "SUMMARIZE parks")
dbGetQuery(con, "SUMMARIZE species")

### das ist ja doof - die Column Names haben Leerzeichen. Das müssen wir ändern, sonst klappt vieles nicht. Also erst mal Tabellen wieder löschen. 
dbExecute(con, "DROP TABLE parks")
dbExecute(con, "DROP TABLE species")

# und nun mit dem Zusatz "normalize_names=True" einlesen
dbExecute(con, "CREATE TABLE parks AS SELECT * FROM read_csv('parks.csv', normalize_names=True)")
dbExecute(con, "CREATE TABLE species AS SELECT * FROM read_csv('species.csv',normalize_names=True) ")

# hat's geklappt? 
dbGetQuery(con, "SUMMARIZE parks")
dbGetQuery(con, "SUMMARIZE species")

######################
#### 2. Abfragen mir SQL
######################


# Jetzt können wir eine Abfrage mit SQL machen. SQL ist recht nah an unserer Sprache: Wähle mir Spalte X aus Tabelle Y, drückt man mit SQL als "SELECT X FROM Y" aus. Wenn wir z.B. das Feld "Park_Name" aus der Tabelle "species" wählen wollen, müssen wir schreiben "SELECT Park_Name FROM species". Hier gibt es viele Beispiele für SQL: https://duckdb.org/docs/stable/sql/introduction

# damit das über R läuft, müssen wir es an die Datenbank schicken, das machen wir mit dbGetQuery()
park_names <- dbGetQuery(con, "SELECT Park_Name FROM species")
park_names

# wir zeigen uns die ganze Tabelle * zu den Parks an
dbGetQuery(con, "SELECT * FROM parks")

# wir zählen die Einträge in der Tabelle "species"
dbGetQuery(con, "SELECT COUNT(Park_Name) FROM species")

# wir zählen die Anzahl der Nationalparke (also wie viele eindeutige Einträge es in der Spalte Park_Name gibt)
dbGetQuery(con, "SELECT COUNT(DISTINCT Park_Name) FROM species")

# wir zählen die Anzahl der Einträge pro Nationalpark. mit AS erstellen wir eine neue Spalte, aber nur in der Abfrage
dbGetQuery(con, "SELECT Park_Name, count(Park_Name) AS Count_of_Entries FROM species GROUP BY Park_Name")

# wir können das auch noch mit ORDER BY sortieren
dbGetQuery(con, "SELECT Park_Name, count(Park_Name) AS Count_of_Entries FROM species GROUP BY Park_Name ORDER BY Count_of_Entries")

# wie sieht es mit dem Anteil von Einheimischen und Nichtheimischen Arten aus? 
dbGetQuery(con, "SELECT nativeness, count(nativeness) AS Count_of_nativeness FROM species GROUP BY nativeness")

######################
#### 3. Views, temporäre Tabellen und neue Tabellen
#####################

# bisher haben wir nur Abfragen erzeugt, die wir in ein R-Objekt überführen könnten. Wir können Abfragen aber auch als View speichern. Bei einem View wird nur die Abfrage gespeichert und keine neue Tabelle erzeugt. 

dbGetQuery(con, "CREATE VIEW nativeness AS SELECT nativeness, count(nativeness) AS Count_of_nativeness FROM species GROUP BY nativeness")

# der View nativeness erscheint jetzt als Tabelle in unsere Datenbank
dbListObjects(con) 

# was steht drin
dbGetQuery(con, "SELECT * FROM nativeness")

# wir können den View wieder löschen
dbExecute(con, "DROP VIEW nativeness")

# wir können auch temporäre Tabellen erzeugen. Diesen werden bei schließen der Datenbank nicht gespeichert
dbGetQuery(con, "CREATE TEMP TABLE nativeness AS SELECT nativeness, count(nativeness) AS Count_of_nativeness FROM species GROUP BY nativeness")

dbListTables(con) 

dbDisconnect(con, shutdown = TRUE)

con <- dbConnect(duckdb(), dbdir = "arten_NP.duckdb", )

dbListObjects(con) 


# wir können auch permanente Tabellen erzeugen. 
dbGetQuery(con, "CREATE TABLE nativeness AS SELECT nativeness, count(nativeness) AS Count_of_nativeness FROM species GROUP BY nativeness")

dbListObjects(con) 

dbGetQuery(con, "SELECT * FROM nativeness")

# wir können den wieder löschen
dbExecute(con, "DROP TABLE nativeness")

dbListObjects(con) 

######################
#### 4. Komplexere Aufgabe: Karte der Nationalparke mit Anteil der nicht-heimischen Arten 
#####################

# dafür erstellen wir einen View mit der Anzahl aller einheimischen Arten pro Park. Wir benutzen dafür den WHERE Befehl (WHERE nativeness='Native'). Gruppieren tun wir nach Parkname

dbExecute(con, "CREATE VIEW native AS SELECT Park_Name, count(nativeness) AS native FROM species WHERE nativeness='Native' AND occurrence='Present' GROUP BY Park_Name")

dbGetQuery(con,"SELECT * FROM native")

#  wir erstellen dann einen View mit der Anzahl aller nichteinheimschen Arten pro Park. (WHERE nativeness='Not Native')  
dbExecute(con, "CREATE VIEW notnative AS SELECT Park_Name, count(nativeness) AS nonative FROM species WHERE nativeness='Not Native' AND occurrence='Present' GROUP BY Park_Name")

dbGetQuery(con,"SELECT * FROM notnative")

# gibt es überhaupt überall einheimische und nicht-heimische Arten?
dbGetQuery(con, "SELECT COUNT(native) FROM native")
dbGetQuery(con, "SELECT COUNT(nonative) FROM notnative")
# da scheint es in zwei Parks keine nicht-heimischen Arten zu geben. Wir müssen das beachten, wenn wir die Tabellen verknüpfen

# in Datenbanken werden Tabellen über den Befahl Join verknüpft. Das ist vielleicht aus ArcGIS bekannt. In R ähnelt der Befehl merge dem JOIN Befehl. Die Syntax ist folgende: 

dbGetQuery(con, "SELECT native.park_name, native.native, notnative.nonative FROM native
JOIN notnative
  ON native.park_name = notnative.park_name;")
# diese Tabelle enthält aber nur 54 Einträge, da die Tabelle notnative nur 54 Einträge hatte. Beheben können wir das mit LEFT JOIN - dadurch kann man sicher gehen, dass das Ergebnis genau so viele Einträge wie die erste angegebene Tabelle hat. 

dbGetQuery(con, "SELECT native.park_name, native.native, notnative.nonative FROM native
LEFT JOIN notnative
  ON native.park_name = notnative.park_name;") 
# jetzt stehe in dieser Abfrage zwei mal NA drin. Wir möchten das gerne mit 0 überschreiben, damit wir einen Anteil berechnen können. Das geht aber nur, wenn wir die Abfrage als neue Tabelle speichern. Es genügt uns eine temporäre Tabelle. 

dbExecute(con, "CREATE TEMP TABLE tmp AS SELECT native.park_name, native.native, notnative.nonative FROM native
LEFT JOIN notnative
  ON native.park_name = notnative.park_name;")

dbGetQuery(con,"SELECT * FROM tmp")

# wo steht überall NA? 
dbGetQuery(con, "SELECT * FROM tmp WHERE nonative IS NULL")

# da wollen wir mit dem SQL-Befehl UPDATE eine 0 rein schreiben 
dbExecute(con, "UPDATE tmp SET nonative = 0 WHERE nonative IS NULL")

# hats geklappt? 
dbGetQuery(con, "SELECT * FROM tmp")

# jetzt hängen wir eine neue Spalte für den Anteil dran. Wir müssen bei einer Datenbank das genaue Inhaltsformat angeben.
dbExecute(con, "ALTER TABLE tmp ADD share DECIMAL(4, 2)")

# jetzt schreiben wir da den Anteil rein. 
dbExecute(con, "UPDATE tmp SET share =(nonative/(nonative+native)*100)")

# jetzt verlassen wir die Datenbank-Welt

# wir hängen die Koordinaten aus der parks Tabelle an die tmp Tabelle an und schreiben alles in einen R dataframe 
map_np <- dbGetQuery(con, "SELECT parks.park_name, parks.latitude, parks.longitude, tmp.share FROM parks 
JOIN tmp
  ON parks.park_name = tmp.park_name;")

str(map_np)

# für die Kartendarstellung benötigen wir einige Pakete
library(sf) # falls noch nicht instaliert install.packages("sf")
library(rnaturalearth) # falls noch nicht instaliert install.packages("rnaturalearth")
library(rnaturalearthdata) # falls noch nicht instaliert install.packages("rnaturalearthdata")
library(ggplot2) # falls noch nicht instaliert install.packages("ggplot2")

# Wir wandeln den dataframne in ein räumliches Objekt um
map_np <- st_as_sf(map_np, coords = c("longitude","latitude"), crs=st_crs("EPSG:4326") )

# wir laden die Geometrie von Nordamerika 
namerica <- ne_countries(continent="North America")

# plotten
p <- ggplot()  
p <- p +geom_sf(data=namerica)
p <- p +  geom_sf(data=map_np, aes(colour =share)) 
p <- p +  ggtitle("Location of national parks in the USA\n and share of non native species")
p <- p + scale_colour_distiller(palette ="YlGnBu", direction = 1)
p


######################
#### 5. Komplexere Aufgabe: Nationalparke mit dem Vorkommen von Coyoten 
#####################



dbExecute(con, "CREATE VIEW coyote AS SELECT park_name, FROM species WHERE scientific_name='Canis latrans' AND occurrence='Present'")

dbGetQuery(con, "SELECT * FROM coyote")


coyote_np <- dbGetQuery(con, "SELECT parks.park_name, parks.latitude, parks.longitude, coyote.park_name AS park_name_coyote FROM parks 
LEFT JOIN coyote
  ON parks.park_name = coyote.park_name;")

coyote_np

is.na(coyote_np$park_name_coyote)

coyote_np$coyote_pres[is.na(coyote_np$park_name_coyote)] <- "No"

coyote_np$coyote_pres[!is.na(coyote_np$park_name_coyote)] <- "Yes"

coyote_np$coyote_pres <- as.factor(coyote_np$coyote_pres)



# Wir wandeln den dataframne in ein räumliches Objekt um
map_coyote <- st_as_sf(coyote_np, coords = c("longitude","latitude"), crs=st_crs("EPSG:4326") )

# wir laden die Geometrie von Nordamerika 
namerica <- ne_countries(continent="North America")

# plotten
p <- ggplot()  
p <- p +geom_sf(data=namerica)
p <- p +  geom_sf(data=map_coyote, aes(colour =coyote_pres)) 
p <- p +  ggtitle("Location of national parks in the USA\n with Coyotes")
#p <- p + scale_colour_distiller(palette ="YlGnBu", direction = 1)
p
