from sqlalchemy.types import Integer, Text, String, DateTime
import pandas as pd
import sys
import requests
from sqlalchemy import create_engine

class DataUploader:
    """
    Parametri: 
        db_url: url del database completo di credenziali d'accesso
        resource_url: url della risorsa/API (JSON) da richiedere sul web
    """

    def __init__(self, db_url, resource_url):
        self.db_url = db_url
        self.resource_url = resource_url
        pass

    def request_data(self):
        """
        Parametri:
            resource_url: url della risorsa json
        Output:
            File JSON
        """

        request = requests.get(self.resource_url)

        if request.ok:
            print("Connessione ok (200)")
            try:
                json_data = request.json()
                print(json_data)
                return json_data
            except BaseException as e:
                print("Errore, i dati non sono quelli attesi: " + e)
                print("Dati ricevuti:")
                print(request.content)
        else:
            print("Non c'è connessione")


    def upload_data(self):
        """
        Decrizione:
            Si connette a un db postgree su heroku e carica una tabella
        Output:
            Messaggio di transazione
        """
        try:
            engine = create_engine(self.db_url)  # fai una query SQL attraverso Pandas
            conn = engine.connect()

            json_data = self.request_data()
            new_data_df = pd.DataFrame(data=json_data['data'])
            
            new_data_df.to_sql("liberoQuotidiano",
                            con=engine,
                            if_exists="append",
                            dtype={
                                "id": Integer,
                                "text": Text,
                                "href": Text,
                                "source": String,
                                "date": DateTime
                            }
                            )
            # TEST
            sql_DF = pd.read_sql_table("liberoQuotidiano", con=engine)
            print(sql_DF)

            success_message = "Dati inseriti correttamente"
            return success_message

        except BaseException as e:
            error_message = "Errore di connessione al DB:"
            print(e)
            return error_message

        finally:
            conn.close()
            engine.dispose()

