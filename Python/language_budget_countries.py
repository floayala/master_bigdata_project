from mrjob.job import MRJob
import logging

# Configuración del logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MRCount(MRJob):
    
    def mapper(self, _, linea):
        """
        La función mapea cada línea de entrada y extrae el idioma, país y presupuesto.

        Parámetros
        ----------
        _ : 
            Valor de la clave de entrada (no utilizado).

        linea : str
            Línea de entrada que se procesará.
        """
        campos = linea.split('|')
        
        idioma = campos[2].strip()  
        pais = campos[3].strip()     
        presupuesto_str = campos[4].strip()
        
        try:
            presupuesto = int(presupuesto_str)
        except ValueError:
            logger.warning(f"Valor de presupuesto no válido: {presupuesto_str} para la línea: {linea}")
            return

        if idioma and pais and presupuesto >= 0:
            yield idioma, (pais, presupuesto)

    def reducer(self, key, values):
        """
        Reduce los resultados del mapeo y calcula el presupuesto total y los países únicos.

        Parámetros
        ----------
        key : str
            El idioma que se está procesando.

        values : iterable
            Tuplas de (país, presupuesto) generadas por el mapper.
        """
        # Conjunto para evitar duplicados
        paises = set()
        presupuesto_total = 0

        # Se itera sobre los valores (tupla(pais, presupuesto))
        for pais, presupuesto in values:
            logging.info(f'País: {pais}, Presupuesto: {presupuesto}')
            paises.add(pais)
            presupuesto_total += presupuesto

        yield key, [list(paises), presupuesto_total]  

if __name__ == '__main__':
    MRCount.run()
