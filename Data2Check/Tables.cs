using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Data2Checker
{
    class Tables
    {
        public static DataTable s_Kunde = new DataTable("DU_Kunde");

        public Tables() 
        {
            s_Kunde = new DataTable("DU_Kunde");
            SetColsKunde(s_Kunde);
        }

        static void SetColsKunde(DataTable s_Kunde)
        {
            s_Kunde.Columns.Add("Kunde / Nummer des Kunden");
            s_Kunde.Columns.Add("Profitzentrum");
            s_Kunde.Columns.Add("Adressnummer");
            s_Kunde.Columns.Add("Name 1");
            s_Kunde.Columns.Add("Staat");
            s_Kunde.Columns.Add("Ort");
            s_Kunde.Columns.Add("Suchbegriff im Adressenstamm");
            s_Kunde.Columns.Add("Selektion im Adressenstamm");
            s_Kunde.Columns.Add("Briefanrede");
            s_Kunde.Columns.Add("Anrede");
            s_Kunde.Columns.Add("Titel");
            s_Kunde.Columns.Add("Vorname");
            s_Kunde.Columns.Add("Name 2");
            s_Kunde.Columns.Add("Name 3");
            s_Kunde.Columns.Add("Postleitzahl");
            s_Kunde.Columns.Add("Prefix Ort");
            s_Kunde.Columns.Add("Postfix Ort");
            s_Kunde.Columns.Add("Prefix Straße");
            s_Kunde.Columns.Add("Strasse");
            s_Kunde.Columns.Add("Postfix Straße");
            s_Kunde.Columns.Add("Hausnummer");
            s_Kunde.Columns.Add("Bundesland");
            s_Kunde.Columns.Add("PLZ des Postfachs");
            s_Kunde.Columns.Add("Postfach");
            s_Kunde.Columns.Add("Telefonbuch / Adresse sichtbar im zentralen Telefonbuch");
            s_Kunde.Columns.Add("EMail");
            s_Kunde.Columns.Add("HomePage");
            s_Kunde.Columns.Add("Handy");
            s_Kunde.Columns.Add("Telefon");
            s_Kunde.Columns.Add("Telefax");
            s_Kunde.Columns.Add("Autotelefon");
            s_Kunde.Columns.Add("Suchbegriff im Kundenstamm");
            s_Kunde.Columns.Add("Selektion im Kundenstamm");
            s_Kunde.Columns.Add("Verteilergruppe");
            s_Kunde.Columns.Add("Branche");
            s_Kunde.Columns.Add("Region");
            s_Kunde.Columns.Add("Sachbearbeiter / Person aus dem Benutzerstamm");
            s_Kunde.Columns.Add("Sprache");
            s_Kunde.Columns.Add("ABC-Klasse");
            s_Kunde.Columns.Add("Teilestatistik");
            s_Kunde.Columns.Add("Webshop");
            s_Kunde.Columns.Add("b2b-Bestandsfaktor");
            s_Kunde.Columns.Add("b2b-Lagerort");
            s_Kunde.Columns.Add("Kunde seit");
            s_Kunde.Columns.Add("eigenen Lieferantennummer beim Kunden");
            s_Kunde.Columns.Add("inländische Steuernummer");
            s_Kunde.Columns.Add("Währung");
            s_Kunde.Columns.Add("Umsatzsteuer-Identifikationsnummer");
            s_Kunde.Columns.Add("RechnungsIntervall");
            s_Kunde.Columns.Add("Sammelrechnung");
            s_Kunde.Columns.Add("Rechnung geht an einen anderen Kunden / Kundennummer");    //50
            s_Kunde.Columns.Add("Konzern");
            s_Kunde.Columns.Add("Bezeichnung des Konzerns");
            s_Kunde.Columns.Add("Verband");
            s_Kunde.Columns.Add("Bezeichnung des Verbandes");
            s_Kunde.Columns.Add("Kennzeichnung des Verbandes als Mahnempfaenger");
            s_Kunde.Columns.Add("Kennzeichnung des Verbandes als Zahlungsregulierer");
            s_Kunde.Columns.Add("MitgliedsNr Verb");
            s_Kunde.Columns.Add("Verband mahnen");
            s_Kunde.Columns.Add("Relevant für Zinsrechnung");
            s_Kunde.Columns.Add("Steuergruppe (Inland) mit Steuer");
            s_Kunde.Columns.Add("Steuergruppe (Inland) ohne Steuer");
            s_Kunde.Columns.Add("Steuergruppe EU mit Steuer");
            s_Kunde.Columns.Add("Steuergruppe EU ohne Steuer");
            s_Kunde.Columns.Add("Steuergruppe Ausland mit Steuer");
            s_Kunde.Columns.Add("Steuergruppe Ausland ohne Steuer");
            s_Kunde.Columns.Add("Zahlungsart");
            s_Kunde.Columns.Add("Zahlungsavis");
            s_Kunde.Columns.Add("Zahlungsziel");
            s_Kunde.Columns.Add("Kreditlimit");
            s_Kunde.Columns.Add("Kreditlimit überwachen");
            s_Kunde.Columns.Add("Mahnverfahren");
            s_Kunde.Columns.Add("Mindestmahnbetrag");
            s_Kunde.Columns.Add("Verzugstage");
            s_Kunde.Columns.Add("Verzugstage sind gültig ab");
            s_Kunde.Columns.Add("Verzugstage wurden manuell erfasst");
            s_Kunde.Columns.Add("Versandart");
            s_Kunde.Columns.Add("Lieferbedingung");
            s_Kunde.Columns.Add("Lieferrestriktion");
            s_Kunde.Columns.Add("Vertragsnummer der Warenkreditversicherung");
            s_Kunde.Columns.Add("Risikonummer der Warenkreditversicherung");
            s_Kunde.Columns.Add("Kreditlimit der Warenkreditversicherung");
            s_Kunde.Columns.Add("Kennzeichen der Warenkreditversicherung");
            s_Kunde.Columns.Add("Letzte Auskunde der Warenkreditversicherung");
            s_Kunde.Columns.Add("Preisliste");
            s_Kunde.Columns.Add("Kunderabattgruppe");
            s_Kunde.Columns.Add("Rabattart sichtbar");
            s_Kunde.Columns.Add("Fracht skontofähig");
            s_Kunde.Columns.Add("Zuschlag Skonto");
            s_Kunde.Columns.Add("Zustand beim Kunden");
            s_Kunde.Columns.Add("Anzahl der auszudruckenden Rechnungen + Kopien");  //90
            s_Kunde.Columns.Add("Anzahl der auszudruckenden Lieferscheine + Kopien");
            s_Kunde.Columns.Add("Nr. des Sammelkontos");
            s_Kunde.Columns.Add("bestätigt");
        }
                  
    }
}
