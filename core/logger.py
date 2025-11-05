import pandas as pd
import json
from datetime import datetime
import os


class EvalLogger:
    _instance = None  # Singleton-Instanz

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, agent_id: str = "default", log_dir: str = "."):
        if hasattr(self, "_initialized") and self._initialized:
            return  # Bereits initialisiert
        self.agent_id = agent_id
        self.log_dir = log_dir
        self.logs = []
        self.current_session = None
        self._initialized = True

    def start_session(self, question: str):
        """Startet eine neue Logging-Session für eine Frage."""
        self.current_session = {
            "session_id": datetime.now().isoformat(),
            "agent_id": self.agent_id,
            "user_question": question,
            "attempts": []
        }

    def log_attempt(self, *, generated_sql, execution_success=None, sql_result=None): #executed_sql=None,
        """Loggt einen Versuch (erster Versuch oder Retry) automatisch nummeriert."""
        if self.current_session is None:
            raise ValueError("Keine aktive Session. Rufe start_session() zuerst auf.")

        attempt_number = len(self.current_session["attempts"]) + 1
        attempt = {
            "attempt_number": attempt_number,
            "timestamp": datetime.now().isoformat(),
            "generated_sql": generated_sql,
            #"executed_sql": executed_sql,
            "execution_success": execution_success,
            "sql_result": sql_result
        }
        self.current_session["attempts"].append(attempt)

    def log_final_answer(self, natural_answer: str):
        """Logs the final natural language answer."""
        if self.current_session is not None:
            self.current_session["final_answer"] = natural_answer
        else:
            print("Warning: No active session for logging final answer")

    def end_session(self): 
        """Beendet die Session und speichert sie in Logs."""
        if self.current_session is not None:
            self.current_session["total_attempts"] = len(self.current_session["attempts"])
            self.current_session["final_success"] = (
                self.current_session["attempts"][-1]["execution_success"]
                if self.current_session["attempts"] else False
            )
            self.logs.append(self.current_session)
            self.current_session = None

    def to_csv(self, filename: str = None, append: bool = False):
        """
        Speichert die Logs als CSV in einem flachen Format für bessere Analyse.
        Jeder Versuch wird als separate Zeile gespeichert.
        """
        if filename is None:
            filename = f"eval_log_{self.agent_id}.csv"

        # Output-Ordner erstellen falls nicht vorhanden
        output_dir = os.path.join(self.log_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        path = os.path.join(output_dir, filename)
        
        # Flache Struktur für CSV erstellen
        flat_logs = []
        for session in self.logs:
            for attempt in session["attempts"]:
                flat_entry = {
                    "session_id": session["session_id"],
                    "agent_id": session["agent_id"],
                    "user_question": session["user_question"],
                    "total_attempts": session["total_attempts"],
                    "final_success": session["final_success"],
                    "attempt_number": attempt["attempt_number"],
                    "attempt_timestamp": attempt["timestamp"],
                    "generated_sql": attempt["generated_sql"],
                    #"executed_sql": attempt["executed_sql"],
                    "execution_success": attempt["execution_success"],
                    "sql_result": attempt["sql_result"],
                    "natural_answer": session.get("final_answer")
                }
                flat_logs.append(flat_entry)
        
        df = pd.DataFrame(flat_logs)
        
        if append and os.path.exists(path):
            df.to_csv(path, mode="a", header=False, index=False)
        else:
            df.to_csv(path, index=False)

    def to_json(self, filename: str = None, append: bool = False):
        """
        Speichert die Logs als JSON mit der vollständigen Session-Struktur.
        """
        if filename is None:
            filename = f"eval_log_{self.agent_id}.json"
        
        # Output-Ordner erstellen falls nicht vorhanden
        output_dir = os.path.join(self.log_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        path = os.path.join(output_dir, filename)
        
        if append and os.path.exists(path):
            # Bestehende Logs laden und erweitern
            try:
                with open(path, "r", encoding="utf-8") as f:
                    existing_logs = json.load(f)
                combined_logs = existing_logs + self.logs
            except (json.JSONDecodeError, FileNotFoundError):
                combined_logs = self.logs
        else:
            combined_logs = self.logs
        
        with open(path, "w", encoding="utf-8") as f:
            json.dump(combined_logs, f, ensure_ascii=False, indent=2)
