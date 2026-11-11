import glob
import json
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Commande magique pour forcer l'affichage dans le notebook Jupyter
%matplotlib inline

def resolve_base():
    """Dossier racine partagé avec le consommateur de la cellule 6."""
    candidates = [os.getenv("HDFS_BASE"), "/hdfs-data",
                  os.path.join(os.path.expanduser("~"), "work", "hdfs-data"),
                  os.path.abspath("hdfs-data")]
    for path in filter(None, candidates):
        try:
            os.makedirs(path, exist_ok=True)
            probe = os.path.join(path, ".write_test")
            with open(probe, "w") as f:
                f.write("ok")
            os.remove(probe)
            return path
        except OSError:
            continue
    raise RuntimeError("Aucun dossier inscriptible pour lire les données.")

WMO_CODES = {
    0: "Ciel dégagé", 1: "Plutôt dégagé", 2: "Partiellement nuageux", 3: "Couvert",
    45: "Brouillard", 48: "Brouillard givrant", 51: "Bruine légère", 53: "Bruine modérée",
    55: "Bruine dense", 56: "Bruine verglaçante légère", 57: "Bruine verglaçante dense",
    61: "Pluie faible", 63: "Pluie modérée", 65: "Pluie forte", 66: "Pluie verglaçante légère",
    67: "Pluie verglaçante forte", 71: "Neige faible", 73: "Neige modérée", 75: "Neige forte",
    77: "Grains de neige", 80: "Averses faibles", 81: "Averses modérées", 82: "Averses violentes",
    85: "Averses de neige faibles", 86: "Averses de neige fortes", 95: "Orage",
    96: "Orage avec grêle légère", 99: "Orage avec forte grêle",
}

def load_logs(base=None):
    """Lit tous les fichiers /hdfs-data/*/*/alerts.json dans un DataFrame pandas."""
    base = base or resolve_base()
    rows = []
    for path in glob.glob(os.path.join(base, "*", "*", "alerts.json")):
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
    return df.sort_values("event_time").reset_index(drop=True)

def most_frequent_code(df):
    """Code météo le plus fréquent par pays."""
    out = (df.groupby(["country", "weather_code"]).size().rename("occurrences").reset_index()
             .sort_values(["country", "occurrences", "weather_code"], ascending=[True, False, True])
             .drop_duplicates("country"))
    out["description"] = out["weather_code"].map(WMO_CODES).fillna("Inconnu")
    return out.reset_index(drop=True)

def make_dashboard(df, save_path=None):
    # Figure plus large pour s'adapter aux 3 jours de données
    fig, axes = plt.subplots(2, 2, figsize=(18, 11))
    fig.suptitle("Tableau de bord Météo - Historique sur 3 jours (HDFS)", fontsize=16, fontweight="bold")

    # Formateur de dates pour l'axe X (affichage Jour + Heure)
    date_format = mdates.DateFormatter('%m-%d %H:%M')

    # 1. Évolution de la température (Courbes sinusoïdales sur 3 jours)
    ax1 = axes[0, 0]
    for city, g in df.groupby("city"):
        ax1.plot(g["event_time"], g["temperature"], linewidth=1.5, label=city)
    ax1.set_title("Évolution de la température (3 derniers jours)", fontsize=12, fontweight="bold")
    ax1.set_ylabel("Température (°C)")
    ax1.xaxis.set_major_formatter(date_format)
    ax1.legend(fontsize=9, loc='upper left')
    ax1.grid(True, linestyle='--', alpha=0.5)

    # 2. Évolution de la vitesse du vent
    ax2 = axes[0, 1]
    for city, g in df.groupby("city"):
        ax2.plot(g["event_time"], g["windspeed"], linewidth=1.2, linestyle='-', label=city)
    ax2.set_title("Évolution de la vitesse du vent (3 derniers jours)", fontsize=12, fontweight="bold")
    ax2.set_ylabel("Vitesse (m/s)")
    ax2.xaxis.set_major_formatter(date_format)
    ax2.legend(fontsize=9, loc='upper left')
    ax2.grid(True, linestyle='--', alpha=0.5)

    # Rotation propre des étiquettes de dates pour les graphiques temporels
    for ax in (ax1, ax2):
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=25, ha='right')

    # 3. Nombre d'alertes vent et chaleur par niveau
    ax3 = axes[1, 0]
    levels = ["level_0", "level_1", "level_2"]
    wind = df["wind_alert_level"].value_counts().reindex(levels, fill_value=0)
    heat = df["heat_alert_level"].value_counts().reindex(levels, fill_value=0)
    x = range(len(levels))
    w = 0.38
    b1 = ax3.bar([i - w / 2 for i in x], wind.values, w, label="Vent", color="#4C78A8")
    b2 = ax3.bar([i + w / 2 for i in x], heat.values, w, label="Chaleur", color="#E45756")
    ax3.bar_label(b1, padding=3)
    ax3.bar_label(b2, padding=3)
    ax3.set_xticks(list(x), levels)
    ax3.set_title("Volume total des alertes par niveau (72h)", fontsize=12, fontweight="bold")
    ax3.set_ylabel("Nombre d'occurrences")
    ax3.legend(fontsize=10)
    ax3.grid(axis='y', linestyle='--', alpha=0.5)

    # 4. Code météo le plus fréquent par pays
    ax4 = axes[1, 1]
    top = most_frequent_code(df)
    labels = [f"{r.country}\n({r.description})" for r in top.itertuples()]
    bars = ax4.barh(labels, top["occurrences"], color="#59A14F")
    ax4.bar_label(bars, padding=3)
    ax4.invert_yaxis()
    ax4.set_title("Code météo le plus fréquent par pays", fontsize=12, fontweight="bold")
    ax4.set_xlabel("Nombre d'heures / relevés")
    ax4.grid(axis='x', linestyle='--', alpha=0.5)

    fig.tight_layout(rect=(0, 0, 1, 0.95))
    
    if save_path:
        fig.savefig(save_path, dpi=150)
        print(f"Graphique haute résolution enregistré : {save_path}")
        
    plt.show()
    return fig

if __name__ == "__main__":
    data = load_logs()
    if data.empty:
        raise SystemExit("Aucune donnée trouvée. Vérifiez que l'Exercice 6 a bien écrit les fichiers.")
    
    print(f"Succès : {len(data)} lignes chargées pour {data['city'].nunique()} villes sur 3 jours.")
    print("\n--- Code météo le plus fréquent par pays ---")
    print(most_frequent_code(data).to_string(index=False))
    print("\nGénération du tableau de bord adapté...")
    
    make_dashboard(data, save_path="meteo_dashboard_3days.png")