# Einführung 

### Übersicht
In diesem Projekt wurde ein LLM-gestützter Workflow - im folgenden "Analyst" genannt - entwickelt, basierend auf dem Langgraph-Framework, der SQL-Abfragen auf eine BigQuery-Datenbank ausführt. 

### Achtung: es sind nicht alle Dateien öffentlich. Die nachfolgende Beschreibung bezieht sich auf das vollständige Projekt.

### Projektstruktur (wo liegt was im Repository?)
Im Ordner 'agent' findet sich das komplette Projekt. Er ist unterteilt in die Ordner 'core' und 'notebooks' (alle weiteren Ordner und Dateien dienen der Dokumentation in Sphinx oder der Paketorganisation!):
+ core: hier finden sich die verschiedenen Analysten (z.B. 'agent_A'), die dann in 'notebooks' vertestet werden.
<br>In 'nodes' sind die Module enthalten, die die Knoten des Workflows definieren (z.B. 'answer.py', 'enrich_schema.py'), die Analysten greifen auf diese zu.
<br>In 'context' finden sich json-Dateien mit Informationen zur Datenbank, die dem Analysten im Laufe des Workflows zur Verfügung gestellt werden (z.B. 'attributes.json').
+ notebooks: hier finden sich die Notebooks zu den Tests der einzelnen Analysten (z.B. 'test_A.ipynb') sowie zur Evaluation ('evaluation.ipynb').
<br>In 'input' liegt der Grundwahrheit-Datensatz, der die Testfragen, sowie den gewünschten SQL-Output und Abfrage-Ergebnisse beinhaltet ('fragen.json').
<br>In 'output' liegen die Testergebnisse für jeden Analysten als CSV- sowie als JSON-Datei.

### Ausführung
Der Analyst wurde in den Test-Notebooks jeweils in einer individuellen Version mittels 60 Fragen getestet. Für die Ausführung werden individuell Zugänge zur Schnittstelle eines Sprachmodells benötigt sowie ein Zugang zur Datenbank in BigQuery.
<br>Die verschiedenen Versionen des Analysten sind gemäß einer Ablation-Study gestaltet. 
<br>Die Auswertung findet vergleichend zwischen Analysten bzgl. der Komponenten Kontext, Retry und Prompting sowie allgemein über alle Analysten, in Bezug auf unterschiedliche Fragenformulierung, unterschiedlich komplexe SQL und anhand einer Fehleranalyse im Notebook evaluation.ipynb statt.

### Dokumentation
Es ist eine Dokumentation in Sphinx vorhanden. Hierzu über die Konsole im Ordner agent den Befehl "make html" ausführen. Dann im neu entstandenen Ordner _build den Ordner html und dann die Datei index.html öffnen.
