# History-Adapter
Der history-Adapter dient zum loggen von Datenpunkten. Deren Statusverlauf im
JSON Format in zwei Schritten gespeichert wird: Zuerst werden die Werte im RAM
zwischengespeichert und anschließend beim Erreichen der maximalen Anzahl von
Werten im RAM in das ausgewählte Speicherverzeichnis geschrieben.

## Installation
Eine Instanz des History-Adapters wird über die ioBroker Admin-Oberfläche mit klicken
auf das + Zeichen installiert.

![](media/Installation.PNG)

Nach der Erstellung der Instanz öffnet sich das Konfigurationsfenster.

## Konfiguration

### Storage-Einstellungen

![](media/KonfigStorage.PNG)

**Speicherverzeichnis**
Hier den Pfad zu dem Verzeichnis eingeben, in dem die Dateien gespeichert werden
sollen. Standardeinstellung ist das /iobroker-data Verzeichnis. Absolute
Verzeichnisse wie z.B.: /mnt/history (Linux) oder D:/history (Windows) können
ebenso eingegeben werden.

**Speichere Quelle vom Ereignis mit**
Legt fest ob die Quelle der Datenänderung (der auslösende Adapter) mit gespeichert
werden soll.

**Speichere Ack vom Ereignis mit**
Legt fest ob das “Ack-Flag” mit gespeichert werden soll.

### Standardeinstellungen für Zustände
Hiermit werden Voreinstellungen für die zu überwachenden Datenpunkte festgelegt.
Jeder Wert kann im Datenpunkt selbst nachträglich geändert werden.

![](media/KonfigZustaende.PNG)

**Maximale Anzahl von Werten im RAM**
Nach dem Erreichen dieser Anzahl werden die Werte vom RAM ins Speicherverzeichnis
geschoben. Besonders bei bei Systemen mit SD-Karte kann ein höherer Wert die
Lebensdauer der SD-Karte erhöhen.

**Änderungen ignorieren, bis der Wert für X Millisekunden unverändert bleibt (Entprellzeit)**
Dies ist der Mindestabstand in Millisekunden bis wieder ein Wert geschrieben
wird und dient zum Schutz vor zu häufigen Änderungen eines Wertes.

**trotzdem gleiche Werte aufzeichnen (Sekunden)**
sollen bei gleichem Wert von Zeit zu Zeit trotzdem diese (unveränderten) Werte
gespeichert werden, kann hier eine Zeitspanne in Sekunden festgelegt werden,
wie häufig dieses geschehen soll. Dementsprechend bedeutet die Eingabe 0, dass
kein doppelter Wert gespeichert werden soll.

**Minimale Differenz zum letzten Wert**
sollen bei ständig wechselnden Werten trotzdem diese (geänderten) Werte nicht
gespeichert werden, kann hier ein Mindestwert festgelegt werden, den sich der Wert
ändern muss, damit wieder ein neuer Wert gespeichert wird. Dies ist beispielsweise
bei Strommesssteckdosen sinnvoll, bei dem nicht jede leichte Veränderung geloggt
werden soll. Dementsprechend bedeutet die Eingabe 0, dass jeder Wert gespeichert
werden soll.

**Storage Vorhaltezeit**
Legt fest, wie lange die Werte gespeichert werden sollen, nach der eingestellten
Zeit werden sie gelöscht (keine automatische Löschung, 2 Jahre, 1 Jahre, …, 1 Tag).

Schreibe NULL-Werte an Start-Stop-Grenzen
??



## Einstellungen für Datenpunkte
Die Einstellungen für den zu loggenden Datenpunkt werden in dem Reiter „Objekte“
bei dem entsprechenden Datenpunkt rechts in der Spalte über das Schraubenschlüssel-
symbol durchgeführt.
![](media/Datenpunkt.PNG)

Das Konfigurationsmenü öffnet sich:

![](media/DatenpunktEinstellung.PNG)

**Aktiviert**
Logging des Datenpunktes aktivieren

**Nur Änderungen aufzeichnen**
Es werden nur Werte gespeichert, wenn sich der Wert des Datenpunktes ändert und
spart somit Speicherplatz. Alle weiteren Einstellungen sind gemäß den Standard-
einstellungen für Zustände voreingestellt und können hier nochmals angepasst werden.

**Alias-ID**
Wenn angeben, werden z.B. nachdem ein Geräte- oder Datenpunktnamen geändert wurde,
die Daten immer noch mit der alten ID protokolliert.

**Mehrere Datenpunkte loggen**
Um mehrere Datenpunkte auf einmal zu loggen, lassen sich über Filterfelder in der
Titelzeile die Datenpunkte so filtern, dass man z.B. nur die „State“ Datenpunkte
herausfiltert, um sie dann gemeinsam alle zu loggen.

>Vorsicht: Bei großen Installationen kann es viele tausend Datenpunkte vom Typ State
geben und das erstellen dauert entsprechend lange. Das beschriebene Vorgehen dient
nur als Beispiel!


1. Hierzu die Ansicht der Objekte in Listenansicht ändern
2. den Filterbegriff state in der Spalte Typ auswählen
3. Den Gabelschlüssel rechts oben anklicken und das Konfigurationsmenü für die
Einstellungen der log-Parameter öffnet sich
*![](media/Datenpunkte.PNG)*
4. Das loggen für alle gefilterten Datenpunkte auf einmal aktivieren
5. Weitere Parameter wie „nur Änderungen“ und Vorhaltezeit für alle gefilterten
Datenpunkte einheitlich auswählen
6. Die Änderungen speichern
![](media/DatenpunktEinstellungen.PNG)

## Bedienung
### Filtern
Wird unter Objekte in der Titelzeile rechts unter Einstellungen “mit” oder
"history.0" ausgewählt, werden nur noch Datenpunkte angezeigt, für die das
Logging aktiv ist:

![](media/Bedienung.PNG)

### Werte anzeigen
Ein Klick auf das Schraubenschlüsselsymbol im Datenpunkt des Objektes öffnet die
Einstellungen erneut und unter Tabelle erscheinen die bereits geloggten Daten:

![](media/DatenpunktTabelle.PNG)

Mit dem runden Pfeilsymbol können die Daten aktualisiert werden und mit dem Pfeil
nach unten Symbol die geloggten Daten als csv-Datei heruntergeladen werden.

### Grafiken
Bei installiertem flot oder Rickshaw Adapter wird im Reiter Grafik der grafische
verlauf angezeigt:

![](media/DatenpunktGrafik.PNG)
