import sqlite3
import datetime
import pickle
import random
import time
import os


### Tarkista, että tietokanta on luotu (tiedosto löytyy). Virhe, jos ei.
def database_exists(db_filename):
    if os.path.exists(db_filename):
        return True
    else:
        print("VIRHE: Tietokantaa ei ole luotu")
        return False


### Tulosta suoritusaika. Pyydettäessä palauta myös aika, josta uuden suorituksen kestoa aletaan laskea.
def get_time(start_time, phase, return_time=True):
    print("Vaihe {}: {} ms".format(phase, round((time.perf_counter() - start_time) * 1000)))
    if return_time:
        return time.perf_counter()


### Tulosta paketin tapahtumat "nätimpänä" käyttäjälle.
def pprint_events(events):
    string = ""
    for event in events:
        string += ", ".join(event) + "\n"
    print(string)


### Tulosta paketin tapahtumien määrä "nätimpänä" käyttäjälle.
def pprint_events_counts(packages, event_counts):
    string = ""
    for package_code, count in zip(packages, event_counts):
        word_form = (" tapahtuma" if count[0] == 1 else " tapahtumaa")
        string += package_code[0] + ", " + str(count[0]) + word_form + "\n"
    print(string)


### Luo pickle-tiedosto listasta (tehokkuustestiä varten).
def build_pickle(alist, file_name):
    with open(file_name, "wb") as f:
        pickle.dump(alist, f)


### Avaa pickle-tiedosto.
def open_pickle(file_name):
    with open(file_name, "rb") as f:
        alist = pickle.load(f)
    return alist



class DB(object):
    def __init__(self):
        self.conn = None
        self.cursor = None
    
    ### Avaa tietokanta. Luo uusi tietokanta, jos ei löydy ennestään.
    def open(self, db_filename):
        try:
            conn = sqlite3.connect(db_filename)
            self.conn = conn
            self.cursor = self.conn.cursor()
            self.conn.isolation_level = None
        except sqlite3.Error:
            return False
        return self.conn, self.cursor

    ### Luo taulut tietokantaan.
    def create_tables(self, printable=True):
        self.cursor.execute("CREATE TABLE IF NOT EXISTS Places (id INTEGER PRIMARY KEY, name TEXT)")
        self.cursor.execute("CREATE TABLE IF NOT EXISTS Customers (id INTEGER PRIMARY KEY, name TEXT)")
        self.cursor.execute(
            "CREATE TABLE IF NOT EXISTS Packages \
                (id INTEGER PRIMARY KEY, code TEXT, customer_id INTEGER)"
        )
        self.cursor.execute(
            "CREATE TABLE IF NOT EXISTS Events \
                (id INTEGER PRIMARY KEY, package_id INTEGER, \
                    place_id INTEGER, description TEXT, time TEXT)"
        )
        if printable:
            print("Tietokanta luotu")
    
    ### Luo indeksit.
    def create_indexes(self):
        self.cursor.execute("CREATE INDEX id_customer ON Packages (customer_id)")
        self.cursor.execute("CREATE INDEX id_package ON Events (package_id)")
        self.cursor.execute("CREATE INDEX id_place ON Events (place_id)")
    
    ### Tehokkuustestin vaiheet 1-4.
    def insert_all(self, places, customers, packages, events, start_time):
        self.cursor.execute("BEGIN")
        self.cursor.executemany("INSERT INTO Places VALUES(?,?)", places)
        new_time = get_time(start_time, "1")
        self.cursor.executemany("INSERT INTO Customers VALUES(?,?)", customers)
        new_time = get_time(new_time, "2")
        self.cursor.executemany("INSERT INTO Packages VALUES(?,?,?)", packages)
        new_time = get_time(new_time, "3")
        self.cursor.executemany("INSERT INTO Events VALUES(?,?,?,?,?)", events)
        get_time(new_time, "4", return_time=False)
        self.cursor.execute("COMMIT")
    
    ### Tehokkuustestin vaiheet 5-6.
    def select_count(self, count, table, phase, start_time):
        sql = (
            "SELECT COUNT(id) FROM Packages WHERE customer_id=?" if phase == 5 else
            "SELECT COUNT(id) FROM Events WHERE package_id=?"
        )
        i = 1
        while (i <= count):
            self.cursor.execute(sql, [random.choice(table)[0]])
            i += 1
        get_time(start_time, phase, return_time=False)



class Place(object):
    def __init__(self, cursor, name, idx=None):
        self.cursor = cursor
        self.name = name
        self.idx = idx
    
    # Hae paikan id tietokannasta.
    def get_idx(self):
        self.cursor.execute("SELECT id FROM Places WHERE name=?", [self.name])
        self.idx = self.cursor.fetchone()
        return self.idx
    
    # Lisää paikka tietokantaan, jos ei ole siellä ennestään.
    def add(self):
        if self.idx is None:
            self.cursor.execute("INSERT INTO Places (name) VALUES (?)", [self.name])
            print("Paikka lisätty")
        else:
            print("VIRHE: Paikka on jo olemassa")
    
    # Hae paikan tapahtumien määrä tiettynä päivänä.
    def get_events_count(self, date):
        self.cursor.execute(
            "SELECT COUNT(*) \
                FROM Events LEFT JOIN Places ON Events.place_id = Places.id \
                    WHERE Events.time LIKE ? AND Places.id=?", ['%'+date+'%', self.idx[0]]
        )
        return self.cursor.fetchone()



class Customer(object):
    def __init__(self, cursor, name, idx=None):
        self.cursor = cursor
        self.name = name
        self.idx = idx
    
    # Hae asiakkaan id tietokannasta.
    def get_idx(self):
        self.cursor.execute("SELECT id FROM Customers WHERE name=?", [self.name])
        self.idx = self.cursor.fetchone()
        return self.idx
    
    # Lisää asiakas tietokantaan, jos ei ole siellä ennestään.
    def add(self):
        if self.idx is None:
            self.cursor.execute("INSERT INTO Customers (name) VALUES (?)", [self.name])
            print("Asiakas lisätty")
        else:
            print("VIRHE: Asiakas on jo olemassa")
    
    # Hae asiakkaan paketit.
    def get_packages(self):
        self.cursor.execute(
            "SELECT Packages.code\
                FROM Customers JOIN Packages ON Customers.id = Packages.customer_id \
                    WHERE Packages.customer_id=?", [self.idx[0]]
        )
        return self.cursor.fetchall()



class Package(object):
    def __init__(self, cursor, code, customer_idx=None, idx=None):
        self.cursor = cursor
        self.code = code
        self.customer_idx = customer_idx
        self.idx = idx
    
    # Hae paketin id tietokannasta.
    def get_idx(self):
        self.cursor.execute("SELECT id FROM Packages WHERE code=?", [self.code])
        self.idx = self.cursor.fetchone()
        return self.idx
    
    # Lisää paketti tietokantaan, jos asiakas löytyy.
    def add(self):
        if self.customer_idx is not None:
            self.cursor.execute(
                "INSERT INTO Packages (code, customer_id) \
                    VALUES (?,?)", [self.code, self.customer_idx[0]]
            )
            print("Paketti lisätty")
        else:
            print("VIRHE: Asiakasta ei ole olemassa")
    
    # Hae paketin tapahtumat.
    def get_events(self):
        self.cursor.execute(
            "SELECT Events.time, Places.name, Events.description \
                FROM Events JOIN Places ON Events.place_id = Places.id \
                    WHERE Events.package_id=?", [self.idx[0]]
        )
        return self.cursor.fetchall()
    
    # Hae paketin tapahtumien määrä.
    def get_events_count(self):
        self.cursor.execute(
            "SELECT COUNT(Packages.id) \
                FROM Events JOIN Packages ON Events.package_id = Packages.id \
                    WHERE Events.package_id=?", [self.idx[0]]
        )
        return self.cursor.fetchone()



class Event(object):
    def __init__(self, cursor, package_idx, place_idx, description, idx=None):
        self.cursor = cursor
        self.package_idx = package_idx
        self.place_idx = place_idx
        self.description = description
        self.idx = idx
        self.time = datetime.datetime.now().strftime("%d.%m.%Y %H:%M")

    # Lisää tapahtuma tietokantaan.
    def add(self):
        self.cursor.execute(
            "INSERT INTO Events \
                (package_id, place_id, description, time) VALUES (?,?,?,?)",
                    [self.package_idx[0], self.place_idx[0], self.description, self.time]
        )
        print("Tapahtuma lisätty")



if __name__ == "__main__":
    print("""
    Komennot:
    0. Poistu.
    1. Luo tietokanta.
    2. Lisää uusi paikka.
    3. Lisää uusi asiakas.
    4. Lisää uusi paketti.
    5. Lisää uusi tapahtuma.
    6. Hae kaikki paketin tapahtumat seurantakoodin perusteella.
    7. Hae kaikki asiakkaan paketit ja niihin liittyvien tapahtumien määrä.
    8. Hae annetusta paikasta tapahtumien määrä tiettynä päivänä.
    9. Suorita tietokannan tehokkuustesti
    """)

    db_filename = "tietokanta.db"
    test_db_filename = "testitietokanta.db"
    db = DB()
    test_db = DB()
    command = ""

    while command != "0":
        print("-----------------------\nValitse toiminto (0-9):")
        command = input()

        # Luo tietokanta.
        if command == "1":
            conn, c = db.open(db_filename)
            db.create_tables()
        
        # Tehokkuustesti.
        elif command == "9":
            # Valmistele listat tehokkuustestiä varten (tätä osaa ei lasketa mukaan suoritusaikaan).
            print("Luodaan listoja...")
            if os.path.exists("places.pkl"):
                places = open_pickle("places.pkl")
                customers = open_pickle("customers.pkl")
                packages = open_pickle("packages.pkl")
                events = open_pickle("events.pkl")
            else:
                places = [(i, "P" + str(i)) for i in range(1, 1001)]
                customers = [(i, "A" + str(i)) for i in range(1, 1001)]
                packages = [[i, "K" + str(i), random.choice(customers)[0]] for i in range(1, 1001)]
                events = [
                    [i, random.choice(packages)[0], random.choice(places)[0], "kuvaus",
                        datetime.datetime.now().strftime("%d.%m.%Y %H:%M")] for i in range(1, 1000001)
                ]
                build_pickle(places, "places.pkl")
                build_pickle(customers, "customers.pkl")
                build_pickle(packages, "packages.pkl")
                build_pickle(events, "events.pkl")
            
            # Aloita varsinainen testi.
            print("Suoritetaan tehokkuustestiä...")

            # Luo erillinen tietokanta tehokkuustestiä varten.
            test_conn, test_c = test_db.open("testitietokanta.db")
            test_db.create_tables(printable=False)

            # Vaiheet 1-4:
            # 1) Tietokantaan lisätään tuhat paikkaa nimillä P1, P2, P3, jne.
            # 2) Tietokantaan lisätään tuhat asiakasta nimillä A1, A2, A3, jne.
            # 3) Tietokantaan lisätään tuhat pakettia, jokaiselle jokin asiakas.
            # 4) Tietokantaan lisätään miljoona tapahtumaa, jokaiselle jokin paketti.
            t0 = time.perf_counter()
            test_db.insert_all(places, customers, packages, events, t0)
            #test_db.create_indexes()

            # Vaiheet 5-6:
            # 5) Suoritetaan tuhat kyselyä, joista jokaisessa haetaan jonkin asiakkaan pakettien määrä.
            # 6) Suoritetaan tuhat kyselyä, joista jokaisessa haetaan jonkin paketin tapahtumien määrä.
            t1 = time.perf_counter()
            test_db.select_count(1000, customers, "5", t1)
            t2 = time.perf_counter()
            test_db.select_count(1000, packages, "6", t2)

            # Sulje yhteys ja poista tietokanta (tiedosto).
            test_conn.close()
            os.remove(test_db_filename)
        
        # Muut komennot.
        else:
            # Tarkista ensin, että tietokanta löytyy.
            if database_exists(db_filename):
                conn, c = db.open(db_filename)

                if command == "2":
                    print("Anna paikan nimi:")
                    name = input()
                    place = Place(c, name)
                    place.get_idx()
                    place.add()

                elif command == "3":
                    print("Anna asiakkaan nimi:")
                    name = input()
                    customer = Customer(c, name)
                    customer.get_idx()
                    customer.add()

                elif command == "4":
                    print("Anna paketin seurantakoodi:")
                    code = input()
                    package = Package(c, code)
                    package_idx = package.get_idx()

                    # Tarkista, ettei seurantakoodilla ole jo olemassa pakettia. Virhe, jos on.
                    if package_idx is not None:
                        print("VIRHE: Seurantakoodilla on jo paketti")
                    else:
                        print("Anna asiakkaan nimi:")
                        name = input()
                        customer = Customer(c, name)
                        customer_idx = customer.get_idx()
                        package = Package(c, code, customer_idx)
                        package.add()

                elif command == "5":
                    print("Anna paketin seurantakoodi:")
                    code = input()
                    package = Package(c, code)
                    package_idx = package.get_idx()

                    # Tarkista, että seurantakoodi löytyy. Virhe, jos ei löydy.
                    if package_idx is None:
                        print("VIRHE: Pakettia ei ole olemassa")
                    else:
                        print("Anna tapahtuman paikka:")
                        name = input()
                        place = Place(c, name)
                        place_idx = place.get_idx()

                        # Tarkista, että paikka löytyy. Virhe, jos ei löydy.
                        if place_idx is None:
                            print("VIRHE: Paikkaa ei ole olemassa")
                        else:
                            print("Anna tapahtuman kuvaus:")
                            descr = input()
                            event = Event(c, package_idx, place_idx, descr)
                            event.add()

                elif command == "6":
                    print("Anna paketin seurantakoodi:")
                    code = input()
                    package = Package(c, code)
                    package_idx = package.get_idx()

                    # Tarkista, että seurantakoodi löytyy. Virhe, jos ei löydy.
                    if package_idx is None:
                        print("VIRHE: Pakettia ei ole olemassa")
                    else:
                        events = package.get_events()
                        pprint_events(events)
                
                elif command == "7":
                    print("Anna asiakkaan nimi:")
                    name = input()
                    customer = Customer(c, name)
                    customer_idx = customer.get_idx()

                    # Tarkista, että asiakas löytyy. Virhe, jos ei.
                    if customer_idx is None:
                        print("VIRHE: Asiakasta ei ole olemassa")
                    else:
                        packages = customer.get_packages()
                        event_counts = []
                        for p in packages:
                            package = Package(c, p[0], customer_idx)
                            package.get_idx()
                            count = package.get_events_count()
                            event_counts.append(count)
                        pprint_events_counts(packages, event_counts)
                
                elif command == "8":
                    print("Anna paikan nimi:")
                    name = input()
                    place = Place(c, name)
                    place_idx = place.get_idx()
                    
                    # Tarkista, että paikka löytyy. Virhe, jos ei.
                    if place_idx is None:
                        print("VIRHE: Paikkaa ei ole olemassa")
                    else:
                        print("Anna päivämäärä (dd.mm.yyyy):")
                        date = input()
                        count = place.get_events_count(date)
                        print("Tapahtumien määrä:", count[0])
                        
