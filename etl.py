import time
import sys
from layers.bronze.scripts.bronze_layer import run_bronze_layer
from layers.silver.scripts.silver_layer import run_silver_layer
from layers.gold.scripts.gold_layer import run_gold_layer


def main():
    """
    Executa o pipeline ETL completo: Bronze -> Silver -> Gold
    """
    print("="*60)
    print("INICIANDO PIPELINE ETL - RAIS")
    print("="*60)
    
    total_start = time.time()
    
    # Bronze Layer
    print("\n[1/3] Executando Bronze Layer...")
    print("-"*60)
    bronze_start = time.time()
    try:
        run_bronze_layer()
        bronze_time = time.time() - bronze_start
        print(f"Bronze Layer concluída em {bronze_time:.2f} segundos")
    except Exception as e:
        print(f"ERRO na Bronze Layer: {e}")
        sys.exit(1)
    
    # Silver Layer
    print("\n[2/3] Executando Silver Layer...")
    print("-"*60)
    silver_start = time.time()
    try:
        run_silver_layer()
        silver_time = time.time() - silver_start
        print(f"Silver Layer concluída em {silver_time:.2f} segundos")
    except Exception as e:
        print(f"ERRO na Silver Layer: {e}")
        sys.exit(1)
    
    # Gold Layer
    print("\n[3/3] Executando Gold Layer...")
    print("-"*60)
    gold_start = time.time()
    try:
        run_gold_layer()
        gold_time = time.time() - gold_start
        print(f"Gold Layer concluída em {gold_time:.2f} segundos")
    except Exception as e:
        print(f"ERRO na Gold Layer: {e}")
        sys.exit(1)
    
    # Resumo final
    total_time = time.time() - total_start
    print("\n" + "="*60)
    print("PIPELINE ETL CONCLUÍDO COM SUCESSO")
    print("="*60)
    print(f"Bronze Layer: {bronze_time:.2f}s")
    print(f"Silver Layer: {silver_time:.2f}s")
    print(f"Gold Layer:   {gold_time:.2f}s")
    print(f"Total:        {total_time:.2f}s ({total_time/60:.1f} minutos)")
    print("="*60)


if __name__ == "__main__":
    main()

