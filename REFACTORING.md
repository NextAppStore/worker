# Worker-Refactoring — Bericht

**Branch:** `refactor/worker` (6 Commits, Basis `e404ebd`)
**Umfang:** 4 Dateien, +1312 / −1163 Zeilen
**Verhalten:** unverändert — 342 Tests grün, 28/28 Charakterisierungsszenarien byte-identisch zu `main`

---

## 1. Warum überhaupt

Der Worker funktionierte. Refactoring „einfach so" ist laut dem
verwendeten Skill ausdrücklich kein Grund, und anders als beim Backend
gab es hier kein Import-Zyklus-Workaround als Alarmsignal. Der Anlass
war eine explizite Anfrage — aber die vorgeschaltete Analyse (vier
parallele Review-Agenten: Reuse, Simplification, Efficiency, Altitude)
fand konkrete, zählbare Befunde:

| Befund | Ausgangslage |
|---|---|
| Längste Funktion | `deploy_application`, 379 LOC, 6 Verschachtelungsebenen |
| Funktionen > 100 LOC | 5 |
| Legacy-Prädikat ausgeschrieben | **8×**, in zwei nicht äquivalenten Schreibweisen |
| Handgeschriebener Fehler-Logblock | **9×** (`if stdout: … if stderr: … raise`) |
| Task-Präambel kopiert | **4×**, je ~25 Zeilen |
| `raise Exception(...)` | 31× |

Entscheidend war nicht die Menge, sondern dass die Kopien **bereits
auseinandergelaufen** waren:

* `destroy_deployment` loggt `resource_info("git_commit", …)`,
  `redeploy_resource` nicht — ohne erkennbaren Grund.
* Bei `terraform init` loggt `deploy_application` zusätzlich eine
  `ERROR`-Zeile, die anderen drei Tasks nicht.
* Das Legacy-Prädikat existierte als `len(templates) == 1 and …` (Deploy)
  und als `not templates or (len(templates) == 1 and …)` (Destroy,
  Redeploy). Für eine App **ohne** Packer-Template bedeutet das
  Unterschiedliches: Deploy injiziert keine `image_name`-Variable,
  Destroy und Redeploy injizieren ein flaches `image_name=<app_id>-<tag>`.

Genau solche stillen Divergenzen sind das Argument für die Zusammenlegung
— und gleichzeitig die Falle, in die dieses Refactoring zuerst
hineingelaufen ist (siehe Abschnitt 4).

Ein zweiter struktureller Befund: `terraform_executor.py` und
`packer_executor.py` loggen ausführlich über `operation_start` /
`command_output` / `operation_end` — aber auf einen **modulweiten**
`StructuredLogger` ohne Event-Emitter, dessen Puffer niemand ausliest.
Nichts davon erreicht das Deployment-Transkript, das das Frontend
rendert. Deshalb schrieb `tasks.py` dieselbe Protokollierung an neun
Stellen noch einmal von Hand.

---

## 2. Wie — die Methode

### 2.1 Analyse vor der ersten Änderung

Vier Review-Agenten liefen parallel über `app/`, je einer mit einem
Blickwinkel (Wiederverwendung, Vereinfachung, Effizienz, Ebenenwahl).
Ihre Befunde wurden dedupliziert und **jeder einzeln im Code
nachgeprüft** — nicht übernommen. Das war nötig: zwei als „toter Code"
gemeldete Parameter (`var_file`, `extra_env`) werden von den Tests
tatsächlich benutzt; ein gemeldeter Vorschlag, den Console-Sink für
Streaming-Zeilen abzuschalten, hätte `make dev-logs-worker` die
Terraform-Ausgabe genommen.

### 2.2 Baseline

Dev-Stack lief bereits, `worker-dev` seit drei Tagen. Baseline im
Container: **331 Tests grün**, `ruff` / `black` / `isort` sauber.
`mypy app/` scheitert schon auf `main` (fehlende `app/__init__.py`), ist
in CI aber `continue-on-error: true` — also kein Gate und nicht angefasst.

### 2.3 Der Ablauf pro Schritt

Umbauen → `ast.parse` als Syntaxprüfung → `black` / `ruff` → Tests →
Commit. Jeder Commit einzeln lauffähig und einzeln zurücknehmbar.

### 2.4 Suchen-und-Ersetzen mit Sicherung

Jede skriptgesteuerte Ersetzung prüfte, dass das Suchmuster **genau
einmal** existiert, und brach vor dem Schreiben ab, wenn nicht.

Das hat zweimal gegriffen:

* Beim Entfernen von `_cleanup_task_resources` hätte ein Index-basierter
  Schnitt „bis zur nächsten Funktion" **13 344 Zeichen** gelöscht, weil
  inzwischen neue Infrastruktur dazwischenlag. Die Zusicherung brach ab,
  die Datei blieb unversehrt.
* Beim Setzen von `legacy_fallback=True` traf das Muster drei statt zwei
  Stellen — darunter `deploy_application`, das es gerade **nicht**
  bekommen durfte. Ein `ast`-basierter Nachlauf, der jede Fundstelle
  ihrer Funktion zuordnet, hat es korrigiert.

Eine Ersetzung ohne Zusicherung ging prompt still daneben: `_build_image_names`
wurde in `deploy_application` eingesetzt, obwohl es für „keine Templates"
etwas anderes liefert als der ersetzte Inline-Code. Das fiel erst beim
Nachdenken auf, nicht den Tests.

---

## 3. Was — die sechs Commits

| Commit | Inhalt |
|---|---|
| `fe92526` | Risikoarme Einzelfixes + `_build_image_names` als einzige Quelle für Imagenamen |
| `c66b3c8` | `_ImagePlan` / `_plan_images` — Packer-Layout einmal auflösen |
| `635863f` | Stale-Data-Source-Retry in den Executor (**später zurückgenommen**) |
| `0c1b870` | `_require_ok` statt neun handgeschriebener Logblöcke |
| `0199c32` | `_TaskContext` + `ExitStack` statt vier kopierter Task-Gerüste |
| `034fe6e` | Verhalten auf 1:1 zurückgesetzt, per Charakterisierungs-Diff bewiesen |

### `_ImagePlan` — den Sonderfall an der Quelle auflösen

`packer_discovery` ist die einzige Stelle, die weiß, ob ein App-Repo das
flache `packer/template.pkr.hcl`-Layout benutzt oder das
`packer/<key>/`-Layout. Sie warf diese Information weg und behielt nur
drei Strings. Jeder Konsument leitete sie aus der Form der Liste neu ab —
achtmal, für sechs verschiedene Fragen: Imagename, Terraform-Variablenname,
drei Phasennamen, Packer-Arbeitsverzeichnis, Verschachtelung der
Packer-Variablen, Log-Präfix.

`_ImagePlan` trägt jede dieser Antworten aufgelöst. Entfallen: der
`is_legacy`-Parameter durch `_build_one_packer_image`, der Inline-Ternär
fürs Verzeichnis, das `if/else` um die Variablen-Scheibe, und
`_build_image_names`.

Nebeneffekt: `_phases_for_plans` liest die Phasennamen vom Plan ab, statt
sie ein zweites Mal zusammenzubauen. Phasenplaner und Packer-Builder
können sich jetzt nicht mehr darüber uneinig sein, wie eine Phase heißt —
das UI gleicht sie per String ab.

### `_require_ok` — neun Kopien auf eine

Der Block ist immer derselbe: stdout loggen, stderr loggen, werfen.
`_require_ok` nimmt das `(success, stdout, stderr)`-Tupel entgegen und
erledigt es. Der Parameter `log_error` bildet ab, dass **nur** Deploys
drei Terraform-Schritte zusätzlich eine `ERROR`-Zeile schrieben.

### `_TaskContext` — ein Gerüst statt vier

Jede der vier Tasks begann mit ~25 Zeilen identischem Aufbau und endete
mit identischem `except`/`finally`. `_task_context` besitzt das Gerüst,
`_task_preamble` die vier gemeinsamen Eröffnungsphasen,
`_terraform_var_set` das Variablen-Rezept.

`PerTaskCloudsConfig` und die Repo-Bereinigung werden auf einem
`ExitStack` registriert, sobald sie erworben sind. Der LIFO-Abbau
schreddert die Credential-Datei vor dem Entfernen des Klons — genau die
Reihenfolge, die `_cleanup_task_resources` vorher von Hand buchstabierte,
jetzt durch Konstruktion erzwungen. Beide Abbauschritte stufen ihre
eigenen Fehler weiterhin zu Warnungen herab, damit die Bereinigung
niemals das echte Ergebnis der Task überdeckt.

`deploy_application` ist in benannte Schritte zerlegt (`_packer_step`,
`_warn_oversized_vars`, `_cleanup_partial_apply`). Glücklicher
Nebeneffekt: Happy Path und Cleanup-Pfad teilen sich jetzt ein
Variablen-Rezept und unterscheiden sich nur noch durch `strip_files` —
vorher waren es zwei getrennte Aufbauten, die auf allem anderen
auseinanderlaufen konnten.

---

## 4. Wo „Tests grün" nicht genug war

Das ist der wichtigste Abschnitt dieses Berichts.

Nach fünf Commits waren **342 Tests grün**, `ruff` / `black` / `isort`
sauber, die Coverage **gestiegen**. Auf dieser Grundlage wurde das
Ergebnis als „Refactoring" gemeldet. Es war keines: auf Nachfrage nach
1:1-identischem Verhalten stellte sich heraus, dass **13 verschiedene
Verhaltensänderungen** durch die grüne Suite gerutscht waren.

Der Grund ist strukturell. Die Tests prüfen Rückgabewerte und einzelne
Mock-Aufrufe. Sie prüfen **nicht** das Log-Transkript — und genau das
ist beim Worker ein Produkt: es wird gepuffert, über den Celery-Event-Bus
gestreamt, vom Backend persistiert und dem Nutzer im Frontend angezeigt.
Eine geänderte Logzeile ist dort eine geänderte Ausgabe, kein Detail.

### Das zweite Sicherheitsnetz — zu spät gebaut

Ein Charakterisierungs-Harness (`.charharness/characterize.py`, 322
Zeilen) fährt alle fünf Tasks über **28 Szenarien** gegen feste Mocks und
zeichnet auf:

* das vollständige gepufferte Log-Transkript,
* jedes Custom-Event auf dem Celery-Bus (`task-log`, `task-progress`),
* den Rückgabe-Payload bzw. den `Failure`-Payload,
* die geordnete Aufruffolge an Terraform, Packer, OpenStack und git.

Zeitstempel und Tracebacks werden normalisiert. Gegen `main` und gegen
den Branch laufen gelassen, dann gediffed. Jeder Unterschied ist eine
Verhaltensänderung.

**Erster Lauf: 25 von 28 Szenarien abweichend.**

Das Harness fand auch einen Fehler in sich selbst: `lock.release` fehlte
in der Aufrufkette, weil ein `MagicMock.__exit__` — anders als das echte
`PackerBuildLock.__exit__` — kein `release()` aufruft. Ein
Harness-Artefakt, kein Codefehler; korrigiert, bevor daraus eine falsche
Schlussfolgerung wurde.

### Die 13 Befunde

Vollständig zurückgenommen, samt Struktur:

| Änderung | Warum sie nicht bleiben durfte |
|---|---|
| `PACKER_LOG` von fest `"1"` auf opt-in | Ändert Menge **und Inhalt** der Packer-Ausgabe |
| `clean_text` nach statt vor dem Kürzen | Ändert den Inhalt gekürzter Logeinträge |
| `run_buffered` in `output()` / `state_pull()` | Loggt Fehlschläge anders (`warning` statt `exception`) |
| Retry in `TerraformExecutor.destroy` | Degradierte die Warnung still auf den Modul-Logger, wo der **Nutzer sie nie sieht** — und dehnte den Retry auf Deploys Cleanup-Destroy aus |
| `raise ... from e` im Packer-Pfad | Exception-Chaining ändert den Traceback, den das Backend per Regex ausliest |

Struktur behalten, Verhalten wiederhergestellt:

| Änderung | Wiederherstellung |
|---|---|
| `_require_ok` loggte immer `error` | Parameter `log_error`, Default `False` |
| Vereinheitlichte Präambel-Texte | `_task_preamble` nimmt Wortlaut, Commit-Info-Modus und Operation-Framing pro Task |
| `terraform_outputs` auf `{}` normalisiert | Wieder unverändert durchgereicht — Deploy meldet `None`, wenn das Einsammeln selbst fehlschlug; das ist von „keine Outputs deklariert" (`{}`) unterscheidbar |
| Deploys Fehlerpfad sammelte Outputs nicht nach | Wieder `if not outputs: …`, und `local_fallback` beim State-Pull |
| `image_name`-Asymmetrie beseitigt | Wieder da, jetzt als benannter Parameter `legacy_fallback` |
| `_terraform_var_set` extrahierte selbst | Nimmt den Roh-Dict, jede Aufrufstelle behält ihre Schreibweise |
| `teams or {}` | Wieder `if teams is None` |

**Zweiter Lauf: 9 von 28.** Alle neun waren Zeitstempel in
`get_summary()`s `timestamp_range`, die der Normalisierer übersah, weil
sie unter `first`/`last` verschachtelt sind.

**Dritter Lauf, nach korrigiertem Normalisierer und neu aufgenommener
Baseline: 0 von 28.**

### Die Lehre

Der Backend-Bericht nennt als Methode „zwei Sicherheitsnetze **vor** der
ersten Änderung". Hier gab es zunächst nur eines, und das war für diesen
Code nicht ausreichend — der Worker hat kein OpenAPI-Schema, aber er hat
ein Log-Transkript, und das ist genauso ein Vertrag. Das Netz kam erst
auf Nachfrage. Die 13 Befunde wären sonst als „Refactoring" durchgegangen.

---

## 5. Was bewusst nicht angefasst wurde

* **`app/utils/logger.py`, `app/services/packer_executor.py`** — stehen
  nach den Rücknahmen gar nicht mehr im Diff gegen `main`.
* **`openstack_service.py`, `build_lock.py`, `openstack_auth.py`,
  `packer_discovery.py`, `git_service.py`, `crypto.py`, `config.py`,
  `celery_app.py`** — unverändert.
* **`run_buffered` für `packer_executor` und `openstack_service`.**
  Deren 36 Tests mocken `subprocess.run` im eigenen Modul; die Umstellung
  hätte `CompletedProcess`-Stubs in Tupel-Stubs übersetzt — eine
  Schwächung genau des Sicherheitsnetzes, das der Rest dieses Umbaus
  braucht, für ~30 gesparte Zeilen.
* **Typisierte Fehlerhierarchie** (`TerraformError` / `PackerError` mit
  `tool` + `phase`). Ihr echter Nutzen wären strukturierte Felder im
  `Failure`-Payload und ein am Fehlertyp entschiedener Cleanup-Destroy —
  beides Verhaltens- bzw. Protokolländerungen. Ohne Konsument wäre sie
  spekulativ, und die Strings, die sie tragen würde, sind heute im UI
  sichtbar.
* **Console-Sink für Streaming-Zeilen abschalten.** Das Container-Log ist
  die einzige Sicht des Operators, wenn kein Frontend offen ist.
* **`app/__init__.py` ergänzen**, um `mypy` zu reparieren. Vorbestehend,
  in CI nicht scharf geschaltet, nicht Teil des Auftrags.

---

## 6. Befunde, die kein Refactoring sind

Die Review-Agenten fanden mehr, als in Refactoring-Commits gehört.
Unverändert gelassen, hiermit gemeldet:

**Die `image_name`-Asymmetrie.** Eine App ohne Packer-Template deklariert
keine `image_name`-Variable, und `terraform -var` auf eine nicht
deklarierte Variable ist ein Fehler. Destroy und Redeploy schicken sie
trotzdem mit. Entweder ist das ein latenter Fehlschlag, oder solche Apps
existieren in der Praxis nicht. Steht jetzt als benannter Parameter
`legacy_fallback` mit Kommentar im Code, statt versteckt in zwei
Schreibweisen desselben Prädikats.

**Der `Failure`-Transport.** `Failure` serialisiert die gesamte Payload
als JSON in die Exception-Message und pinnt dafür `__repr__` als
Wire-Format fest; das Backend fischt sie per
`re.search(r"Failure\('(.+?)'\)", …)` samt `unicode_escape`-Fallback aus
dem Traceback. Der eigentliche Transportweg — das Result-Backend mit
`result_extended=True` — ist konfiguriert und ungenutzt. Eine
service-übergreifende Protokolländerung.

**Die Roster-Heuristik.** `_reconcile_scoped_vars_to_roster` (125 Zeilen)
errät per Mengenschnitt, welche Terraform-Variablen team- oder
user-scoped sind, obwohl `varScope` im Backend deklariert und validiert —
aber nie mitgeschickt wird.

**Der Log-Puffer.** Jede gestreamte Subprozess-Zeile bleibt bis
Task-Ende im Speicher, wird ins Result-Dict serialisiert und im
Fehlerfall ein zweites Mal per `json.dumps` in `Failure.args[0]` —
inklusive des dort eingebetteten `tf_state`. Bei einem gesprächigen
Packer-Build ist das relevant, zumal `PACKER_LOG=1` fest gesetzt ist.

**Der Build-Lock belegt einen Worker-Slot.** Die Warteschleife schläft
bis zu 25 Minuten per `time.sleep` im Celery-Task-Thread, bei
`worker_prefetch_multiplier=1`.

**Pause/Resume ist sequenziell.** Zwei `openstack`-CLI-Prozesse pro VM,
jeder mit eigenem Keystone-Login; `server_show` ist rein kosmetisch.

**Duplikation zwischen Worker und Backend.** `crypto.py` ist bis auf
Docstrings identisch; `packer_discovery.py` teilt Regex und Regeln mit
`backend/app/services/hcl/packer.py`; `_looks_like_file_var_value` und
der TF-Adress-Regex existieren beidseitig. Real, aber die Auflösung
bräuchte ein geteiltes Paket und zwei Dockerfiles für ~150 Zeilen, die
sich praktisch nie ändern.

---

## 7. Ergebnis

| Metrik | vorher | nachher |
|---|---:|---:|
| `deploy_application` | 379 LOC | **175** |
| `destroy_deployment` | 219 LOC | **109** |
| `redeploy_resource` | 225 LOC | **119** |
| `_run_compute_lifecycle` | 210 LOC | **122** |
| Max. Verschachtelung in `deploy_application` | 6 | **5** |
| Funktionen > 100 LOC | 5 | 5 |
| Legacy-Prädikat ausgeschrieben | 8× | **1×** (die Definition) |
| Handgeschriebener Fehler-Logblock | 9× | **0** |
| Kopierte Task-Präambel | 4× | **1** |
| `raise Exception(...)` in `tasks.py` | 31 | **12** |
| Statements in `tasks.py` | 728 | **602** |
| Coverage `tasks.py` | 84,34 % | **91,20 %** |
| Coverage gesamt | 92,46 % | **95,85 %** |
| Tests | 331 | **342** |
| Verhalten | — | **28/28 Szenarien identisch** |

`app/tasks.py` ist von 1963 auf 1944 Zeilen gegangen — praktisch
unverändert. Das ist kein Versehen, aber es war auch nicht die
Vorhersage: angekündigt waren ~1100 Zeilen. Diese Prognose war falsch.
Duplikation zu entfernen spart weniger Zeilen als erwartet, weil die
extrahierten Helfer explizite Signaturen und Docstrings kosten — reiner
Code geht von 1279 auf 1238 Zeilen zurück, Docstrings steigen von 256 auf
410. Kürzere Funktionen, ein aufgelöster Sonderfall und ein Aufräumpfad,
den man nicht vergessen kann, waren das Ziel; weniger Zeilen nie.

Auch die Zahl der Funktionen über 100 LOC bleibt bei 5 — es sind nur
andere, und die längste ist weniger als halb so lang. `_build_one_packer_image`
(125 LOC) ist die einzige, die durch dieses Refactoring nicht kürzer
geworden ist.

**Anmerkung zu den Commit-Messages:** `0199c32` nennt „94,60 % → 95,85 %".
Die 94,60 % waren gegen eine kontaminierte Baseline gemessen (Branch-Tests
gegen `main`-Code, via `git stash`). Der saubere Wert ist 92,46 %; die
Tabelle oben ist maßgeblich.

---

## Reproduktion

Voraussetzung: der Dev-Stack läuft (`make dev-up` aus `deployment/`).

```bash
# Tests + Coverage
docker compose -f deployment/docker-compose.dev.yml exec -T worker \
  bash -c "cd /app && poetry run pytest -q --cov=app --cov-report=term"

# Linter
docker compose -f deployment/docker-compose.dev.yml exec -T worker \
  bash -c "cd /app && poetry run ruff check . && poetry run black --check . && poetry run isort --check-only ."

# Verhaltensvergleich gegen main
#   1. auf main auschecken, Harness aufnehmen:
docker compose -f deployment/docker-compose.dev.yml exec -T worker \
  bash -c "cd /app && PYTHONPATH=/app poetry run python .charharness/characterize.py" > baseline.json
#   2. auf den Branch wechseln, erneut aufnehmen, diffen:
docker compose -f deployment/docker-compose.dev.yml exec -T worker \
  bash -c "cd /app && PYTHONPATH=/app poetry run python .charharness/characterize.py" > after.json
python3 -c "import json,sys; a=json.load(open('baseline.json')); b=json.load(open('after.json')); \
print(sum(1 for x,y in zip(a,b) if x!=y), 'von', len(a), 'Szenarien abweichend')"
```

> **Offener Punkt:** `.charharness/` ist derzeit per `.gitignore`
> ausgenommen und damit **nicht Teil des Repos**. Die obige Reproduktion
> funktioniert so nur lokal. Da das Harness der einzige Beleg für die
> Verhaltensgleichheit ist — und für künftige Umbauten am Worker
> wiederverwendbar wäre — spricht mehr dafür, es einzuchecken als es
> wegzuwerfen. Das ist noch zu entscheiden.
