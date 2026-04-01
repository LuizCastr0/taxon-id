import requests
import pandas as pd
import os

def fetch_elite_dataset(total_goal=1000):
    print(" Extração de dados do GBIF para criar um datasetde aves com descrições morfológicas detalhadas. ")
    url = "https://api.gbif.org/v1/occurrence/search"
    results = []
    
    # termos técnicos em inglês
    queries = ["plumage", "bill", "wing", "tail", "feathers", "coloration", "specimen"]
    
    for q in queries:
        print(f"\n termo: [{q}]")
        for offset in range(0, 2100, 300):
            params = {
                "classKey": 212,
                "country": "US", # EUA tem muitos registros
                "q": q,
                "limit": 300,
                "offset": offset,
                "occurrenceStatus": "PRESENT"
            }
            
            try:
                resp = requests.get(url, params=params, timeout=15).json()
                records = resp.get('results', [])
                if not records: break
                
                for r in records:
                    desc = r.get("occurrenceRemarks") or r.get("eventRemarks") or r.get("fieldNotes")
                    species = r.get("species")
                    
                    if desc and species and len(desc) > 100:
                        results.append({
                            "species": species,
                            "family": r.get("family"),
                            "description": desc.replace('\n', ' ').strip(),
                            "scientificName": r.get("scientificName"),
                            "country": r.get("country")
                        })
                
                print(f"   > acumulados: {len(results)} registros...")
                if len(results) >= total_goal * 1.5: break
                
            except Exception as e:
                print(f"   ⚠️ Erro: {e}")
                break

    
    df = pd.DataFrame(results).drop_duplicates(subset=['description'])
    
    if not df.empty:
        os.makedirs('data/raw', exist_ok=True)
        df.to_csv('data/raw/aves_eua_filtrado.csv', index=False)
        
        print("\n" + "="*40)
        print(f"numero de descrições: {len(df)}")
        print(f"numero de espécies: {df['species'].nunique()}")
        print("="*40)
    else:
        print("erro: nenhum registro encontrado com esses filtros")

if __name__ == "__main__":
    fetch_elite_dataset(total_goal=1000)