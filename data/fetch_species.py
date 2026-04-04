import requests
import wikipediaapi
import pandas as pd
import time
import re

agent = "TaxonID (silvestre.castro.luiz@gmail.com)"
lingua = 'en'
max_especies = 1000
output = "data/raw/df_borboletas.csv"

def especies_list(target=1000):
    all_species = []
    page = 1
    per_page = 100 # máximo da api
    
    while len(all_species) < target:
        url = f"https://api.inaturalist.org/v1/taxa?taxon_id=47157&rank=species&per_page={per_page}&page={page}&order_by=observations_count"
        try:
            r = requests.get(url, headers={"agent: ": agent}, timeout=10)
            data = r.json()
            
            names = [t['name'] for t in data['results']]
            if not names: break
            
            all_species.extend(names)
            print(f"\n pagina {page} processada ({len(all_species)} nomes encontrados)")
            
            page += 1
            time.sleep(1)
        except Exception as e:
            print(f"\n erro na pagina {page}: {e}")
            break
            
    return all_species[:target]

# extraindo morfologia da wikipedia
def fetch_wiki_morphology(scientific_name, wiki_instance):
    try:
        page = wiki_instance.page(scientific_name)
        if not page.exists(): return None

        text_content = ""
        target_sections = ["Description", "Appearance", "Morphology", "Adult", "Identification", "Characteristics"]
        
        for section in page.sections:
            if any(target in section.title for target in target_sections):
                text_content = section.text
                break
                
        if not text_content or len(text_content) < 150:
            text_content = page.summary

        # limpeza de ruído
        text_content = re.sub(r'\[\d+\]', '', text_content)
        text_content = re.sub(r'\s+', ' ', text_content).strip()

        return {
            "species": scientific_name,
            "text": text_content,
            "url": page.fullurl
        }
    except:
        return None

def pipeline():
    wiki = wikipediaapi.Wikipedia(agent=agent, lingua=lingua)
    
    lista_especies = especies_list(max_especies)
    
    final_data = []
    
    for i, nome in enumerate(lista_especies):
        # progresso 
        percent = (i + 1) / len(lista_especies) * 100
        print(f": [{i+1}/{len(lista_especies)}] {percent:.1f}% | {nome}", end="\r")
        
        res = fetch_wiki_morphology(nome, wiki)
        
        if res and len(res['text']) > 150:
            final_data.append(res)
        
        # nao tirar o delay para nao ser banido da wikipedia
        time.sleep(1.0)

    if final_data:
        df = pd.DataFrame(final_data)
        df.to_csv(output, index=False)
        print(f"\nsalvo em: {output}")
        print(f"\n {len(df)} espécies")
    else:
        print("\n\n erro na coleta")

if __name__ == "__main__":
    pipeline()