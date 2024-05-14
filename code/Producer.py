# PRODUCER :

import pandas as pd
import tkinter as tk
import json, time, datetime, subprocess
from kafka import KafkaProducer
from utils import connection_db, get_data, TOPIC_NAME, SERVER_PORT


# TRAITEMENTS DONNEES 
# Nous allons créer une nouvelle colonne 'opration_type':
#    - les valeurs d'opérations positives sont des 'ajout de stock' 
#    - les opérations négatives sont des 'ventes'
# De plus, nous allons transformer la colonne 'operation_qt' pour avoir des valeurs positives.
# Nous allons aussi formatter la colonne 'operation_date' en format date

# Lecture des données à partir du fichier CSV contenant des opérations
df = pd.read_csv("./data/operations.csv", sep=';')

# Création d'une nouvelle colonne 'operation_type' pour identifier les ventes et les ajout de stock
df['operation_type']=df['operation_qt'].apply(lambda x: 'replenishment' if x>0 else 'sales')

# Transformation des valeurs négatives de 'operation_qt' en valeurs positives
df['operation_qt'] = df['operation_qt'].abs()

# Formatage de la colonne 'operation_date'
df['operation_date'] = pd.to_datetime(df['operation_date'], format='%d/%m/%Y')
df['operation_date'] = df['operation_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
df['operation_date'] = df['operation_date'].astype(str)


# Envoie des données dans le Topic
def send_records(df, producer):
    for i in range(len(df)):
        event = df.iloc[i, :].to_dict()
        producer.send(TOPIC_NAME, value=event)
        # Pause d'une seconde entre l'envoi de chaque enregistrement
        time.sleep(1)
    print('Envoie des données au Topic')


# Creation du Consomer Kafka 
# Spécification de l'adresse du serveur Kafka
# Sérialisation des valeurs des enregistrements en JSON
producer = KafkaProducer(
    bootstrap_servers=[SERVER_PORT],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)


# INTERFACE GRAPHIQUE :
# Création de l'interface graphique avec Tkinter

class Application(tk.Tk):
    def __init__(self, conn, producer):
        # Méthode spéciale appelée lors de la création d'une instance de la classe
        super().__init__()

        # Initialisation des variables membres de la classe pour la gestion des données
        self.product_ID = None
        self.operation_qt = None
        self.operation_type = None
        self.operation_date = None
        self.stock = None
        
        # Connexion à la base de données et récupération des données initiales
        self.conn = conn
        self.df = get_data(self.conn)

        # Création de la première fenêtre de l'application
        self.create_first_window()

    # Méthode pour effacer tous les widgets de la fenêtre
    def clear_window(self):
        # Supprimer les objets de la fenêtre
        for widget in self.winfo_children():
            widget.destroy()

    # Méthode pour centrer la fenêtre sur l'écran 
    def center_window(self):
            self.update_idletasks()
            w = self.winfo_width()
            h = self.winfo_height()
            x = (self.winfo_screenwidth() // 2) - (w // 2)
            y = (self.winfo_screenheight() // 2) - (h // 2)
            self.geometry(f"{w}x{h}+{x}+{y}")

    # Méthode pour valider le montant de l'opération 
    def validate_quantity_operation(self, operation_qt):
        try:
            if float(operation_qt)>0:
                return True
        except ValueError:
            return False
    
    # Méthode pour mettre à jour la base de données avec les nouvelles données de l'opération
    def update_bdd(self):        
        new_df = pd.DataFrame(columns=self.df.columns)
        new_df.loc[len(new_df)] = [self.operation_date, self.product_ID, self.operation_qt, self.stock, self.operation_type]
        send_records(new_df, producer)
        
        #conn = connection_db()
        cursor = conn.cursor()
        conn.commit()
        self.conn = conn
        self.df = get_data(self.conn)
        
    
    # Méthode pour créer la première fenêtre de l'application
    def create_first_window(self):
        self.df = get_data(self.conn)
        
        self.clear_window()
        self.title("Product Connection")
        self.geometry("400x150")
        self.center_window()
        
        # Label et zone de saisie pour entrer l'ID du produit
        self.label = tk.Label(self, text="Enter Product ID :")
        self.label.pack(pady=10)
        
        self.product_entry = tk.Entry(self)
        self.product_entry.pack()
        self.product_entry.focus_set()

        # Bouton pour valider la connexion
        self.submit_button = tk.Button(self, text="Connection", command=self.connection)
        self.submit_button.pack()
        
        # Affichage des numéros de compte disponibles
        txt = f"Product_ID Available : \n{list(self.df['product_ID'].unique())}"
        self.product_accepted = tk.Label(self, text=txt)
        self.product_accepted.pack(pady=7)


    # Méthode pour créer la deuxième fenêtre de l'application (gestion des opérations)
    def create_second_window(self):
        self.df = get_data(self.conn)
        
        self.clear_window()
        self.title("Make Operation")
        self.larg = 400
        self.long = 190
        self.geometry(f"{self.larg}x{self.long}")
        self.center_window()
        
        self.stock = float(self.df[self.df['product_ID']==self.product_ID]['stock'].values[-1])

        # Labels pour afficher le product_ID et le stock disponible
        self.label2 = tk.Label(self, text=f"Product ID : \"{self.product_ID}\"\n")
        self.label2.pack(pady=1)

        self.label3 = tk.Label(self, text=f"Available Stock -->> \"{self.stock}\"\n")
        self.label3.pack(pady=1)

        # Label et zone de saisie pour entrer la quantité de l'opération
        self.quantity_label = tk.Label(self, text="Enter Operation Quantity :")
        self.quantity_label.pack(pady=1)

        self.quantity_entry = tk.Entry(self)
        self.quantity_entry.pack()
        self.quantity_entry.focus_set()

        # Boutons pour effectuer des ventes, des ajout de stock et revenir à la première fenêtre
        self.replenishment_button = tk.Button(self, text="Replenishment", command=self.replenishment)
        self.replenishment_button.place(x=self.larg-320, y=self.long-50)

        self.sales_button = tk.Button(self, text="Sales", command=self.sales)
        self.sales_button.place(x=self.larg-231, y=self.long-50)

        self.back_button = tk.Button(self, text="Back", command=self.create_first_window)
        self.back_button.place(x=self.larg-120, y=self.long-50)
            
    # Méthode pour gérer la connexion à un product_ID  
    def connection(self):
        self.product_ID = self.product_entry.get()
        
        if self.product_ID in self.df['product_ID'].unique():
            self.create_second_window()
        
    # Méthode pour effectuer un ajout de stock
    def replenishment(self):
        operation_qt = self.quantity_entry.get()
        
        if self.validate_quantity_operation(operation_qt):
            self.operation_qt = float(operation_qt)
            self.operation_type = 'replenishment'
            self.stock += self.operation_qt
            self.operation_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            mssg = f"replenishment {self.operation_qt} into product \"{self.product_ID}\" at {self.operation_date} | New Available stock = {self.stock}"
            print(mssg)
            self.update_bdd()
            self.create_second_window()
        
    # Méthode pour effectuer un retrait
    def sales(self):
        operation_qt = self.quantity_entry.get()
        
        if self.validate_quantity_operation(operation_qt):
            self.operation_qt = float(self.quantity_entry.get())
            self.operation_type = 'sales'
            self.stock -= self.operation_qt
            self.operation_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            mssg = f"Selling \"{self.operation_qt}\" of product \"{self.product_ID}\" at \"{self.operation_date}\" | New Available stock = \"{self.stock}\""
            print(mssg)
            self.update_bdd()
            self.create_second_window()
        

if __name__ == "__main__":
    # Connexion à la base de données
    conn = connection_db()
    # Envoie des cinq premiers enregistrements du dataframe `df` au topic Kafka à l'aide du producteur défini.
    send_records(df.iloc[:5, :], producer)
    
    # Lancement de la WebApp
    # Utilisation de subprocess.Popen pour exécuter une commande shell qui lance l'application Streamlit
    # 'shell=True' permet l'exécution de la commande dans le shell. 'stdout' et 'stderr' sont redirigés
    print('Lancement de l\'application web ')
    subprocess.Popen("streamlit run code/web_interface.py", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    # Création d'une instance de l'application Tkinter
    print('Lancement de l\'interface Tkinter')
    app = Application(conn, producer)
    # Démarrage de la boucle principale de Tkinter, qui attend les interactions utilisateur
    app.mainloop()