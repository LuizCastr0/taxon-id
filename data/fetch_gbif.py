import requests
import pandas as pd
import os

def fetch_diverse_data(total_goals=500):
    print("--- Iniciando Coleta Global Diversificada ---")
    url = "https://api.gbif.org/v1/occurrence/search"
    
    # IDs das Classes: 212 (Aves), 359 (Mamíferos), 131 (Anfíbios)
    classes = [212, 359, 131]
    results = []
    seen_species = {} 

    for class_key in classes:  # Nome corrigido aqui
        print(f"Buscando classe ID {class_key}...")
        offset = 0
        
        # Busca até atingir a meta por classe (ex: 200 registros por classe)
        while len([r for r in results if r['class_id'] == class_key]) < (total_goals // len(classes)):
            params = {
                "classKey": class_key,
                "limit": 300,
                "offset": offset,
                "occurrenceStatus": "PRESENT",
                "q": "description" 
            }
            
            try:
                resp = requests.get(url, params=params)
                data = resp.json()
                records = data.get('results', [])
                
                if not records: 
                    break
                
                for r in records:
                    species = r.get("species")
                    # Tenta pegar a descrição em qualquer um desses campos comuns
                    desc = r.get("occurrenceRemarks") or r.get("eventRemarks") or r.get("fieldNotes")
                    
                    if desc and len(desc) > 30 and species:
                        count = seen_species.get(species, 0)
                        if count < 2: # No máximo 2 descrições por espécie para garantir variedade
                            results.append({
                                "class_id": class_key,
                                "species": species,
                                "family": r.get("family"),
                                "description": desc,
                                "country": r.get("country")
                            })
                            seen_species[species] = count + 1
                
                offset += 300
                print(f"Processados {offset} registros da classe {class_key}...")
                if offset > 3000: break 
                
            except Exception as e:
                print(f"Erro na requisição: {e}")
                break

    df = pd.DataFrame(results)
    if not df.empty:
        os.makedirs('data/raw', exist_ok=True)
        df.to_csv('data/raw/taxa_diversity.csv', index=False)
        print(f"\n✅ SUCESSO!")
        print(f"Total de registros: {len(df)}")
        print(f"Total de espécies únicas: {df['species'].nunique()}")
    else:
        print("❌ Nenhum dado encontrado.")

if __name__ == "__main__":
    fetch_diverse_data(total_goals=600)