
  ##Proiect BigData:  Bargan Diana Georgiana
       ### Walmart Recruiting - Store Sales Forescasting

#Incarcare pachete necesare
library(dplyr)
library(lubridate)
library(ggplot2)
library(corrmorant)
library(tidyverse)
library(DBI)
library(purrr)
library(tidyr)
# Install sparklyr from CRAN
install.packages("sparklyr")
# Load the sparklyr library
library(sparklyr)

#Install spark in R :
 # display all available Spark versions
spark_available_versions()
# we install the version that we want:
spark_install(version = "3.5.1")
#we check which version is installed
spark_installed_versions()

#-------- Conectare la Spark --------------- 
Sys.setenv(JAVA_HOME = 'C:/Users/Diana/Desktop/Spark_java/zulu22.30.13-ca-jdk22.0.1-win_x64')

# Connect to a local Spark instance
sc <- spark_connect(master = "local")

###  Pasi pentru incarcarea seturilor de date dintr-o sesiune deja salvata ### 
#Incarcare sesiune salvata
#load("all_dataframes_proiect.RData")

#Copiem dataframes din sesiunea salvata in mediul Spark pentru conectari ulterioare la spark
spark_sales <- sdf_copy_to(sc, r_sales, "spark_sales",overwrite = TRUE)
spark_features <- sdf_copy_to(sc, r_features, "spark_features",overwrite = TRUE)
spark_stores <- sdf_copy_to(sc, r_stores, "spark_stores",overwrite = TRUE)
spark_merge_df <- sdf_copy_to(sc, r_merge_df, "spark_merge_df",overwrite = TRUE)

#cod pentru stergere obiect din spark:
#dbRemoveTable(sc, "sales")

#Comanda pentru deconectare de pe Spark:
spark_disconnect(sc)   

#---------------------------------------------------------------------
    #### Pasi realizati doar la inceput pentru preprocesarea seturilor de date in mediul Spark. 
    ##   Acesti pasi nu au mai fost repetati ulterior deoarece seturile de date curatate
    ##   au fost salvate in fisierul/sesiunea numita : "all_dataframes_proiect.RData". 

#1. Se incarca toate cele 3 seturi de date local
#2. Realizam preprocesarea si curatarea datelor
#3. Imbinam toate cele 3 seturi de date intr-un singur set mare de date. 
#4. Dupa preprocesare, seturile de date salvate in Spark le vom colecta
   #si le vom salva pentru utilizari ulterioare in analizele noastre. 

#Read the CSV files (au fost citite o singura data pentru curatare, dupa nu au mai fost rulate comenzile)
features<-read.csv("features.csv")
sales<-read.csv("train.csv")
stores<-read.csv("stores.csv")

#Read dataframes from local into Spark
spark_sales <- sdf_copy_to(sc, sales, "sales")
spark_features <- sdf_copy_to(sc, features, "features")
spark_stores <- sdf_copy_to(sc, stores, "stores")

#Vedem ce dataframes avem incarcate in Spark.
src_tbls(sc)

#-------------------------------------------------------------------------
        #### Preprocesarea seturilor de date in Sparklyr ####
#--------------------------------------------------------------------------

glimpse(spark_sales)

# Transformarea coloanei Date in tipul as.Date()
spark_sales <- spark_sales %>%
  mutate(Date = as.Date(Date))

#Verificam daca avem valori NA in spark_sales dataframe:
spark_sales %>%
  summarise(across(everything(), ~ sum(as.integer(is.na(.))))) %>%
  collect()

#Descriptive statistics for numerical variables from the sales dataframe
sdf_describe(spark_sales) %>%
  collect() %>%
  pivot_longer(cols = -summary, names_to = "column", values_to = "value") %>%
  pivot_wider(names_from = summary, values_from = value)

###Nu exista valori NA in setul de date, dar la o privire mai atenta, 
##valorile negative sunt vizibile in vanzarile saptamanale !!

### Numaram randurile cu valori negative din coloana Weekly_Sales:
spark_sales %>%
  summarise(count = sum(ifelse(Weekly_Sales < 0, 1, 0)),
            total_rows = n() ) %>%
  mutate(percentage_negative = (count / total_rows) * 100) %>%
  collect()

#Exista 1285 de rânduri cu valori negative din setul de date de 42.1570 de randuri, 
#ceea ce reprezinta doar 0,3% din intregul setul si, prin urmare, le vom elimina.

#------------- Explicatie cod --------------------------------------------
#Am folosit functia ifelse pentru a transforma valorile din Weekly_Sales
#in 1 sau 0 in functie de daca sunt negative sau nu. 
#Apoi, am utilizat functia sum() pentru a aduna aceste valori 1 si 0, obtinand
#astfel nr.de randuri negative. Apoi am calculat calculat procentajul si am colectat datele
#-------------------------------------------------------------------------

# Eliminam valorile negative ale vanzarilor saptamanale:
spark_sales <- spark_sales %>%
  filter(Weekly_Sales > 0)

#Summary vanzari saptamanale:
spark_sales %>%
  summarise(
    count_sales = n(),
    min_sales = min(Weekly_Sales),
    mean_sales = mean(Weekly_Sales),
    median_sales = median(Weekly_Sales),
    max_sales = max(Weekly_Sales),
    sd_sales = sd(Weekly_Sales) ) %>%
  collect() %>%
  print()

#Dimensiunea setului de date dupa preprocesare este:
sdf_dim(spark_sales)

#-----------------------------------------------------------------
                ##Preprocesare features dataframe:

glimpse(spark_features)

spark_features <- spark_features %>%
  mutate(Date = as.Date(Date))

#Verificam daca avem valori NA in sales dataframe:
spark_features %>%
  mutate(across(everything(), ~na_if(., "NA"))) %>%
  summarise(across(everything(), ~ sum(as.integer(is.na(.))))) %>%
  collect() %>%
  print()

#Deoarece nu avem prea multe informatii despre reduceri si cea mai mare 
#parte a acestora sunt NA, nu putem sa inlocuim valorile lipsa cu o
#medie/mediana. In schimb, inlocuim NA cu valoarea 0, astfel incat sa putem 
#utiliza reducerile pentru ulterioarele analize.
spark_features <- spark_features %>%
  mutate(across(everything(), ~na_if(., "NA"))) %>%
  mutate_at(vars(MarkDown1:MarkDown5), ~ ifelse(is.na(.), 0, .))

glimpse(spark_features)

#Descriptive statistics features dataframe:
sdf_describe(spark_features) %>%
  collect() %>%
  pivot_longer(cols = -summary, names_to = "column", values_to = "value") %>%
  pivot_wider(names_from = summary, values_from = value) %>%
  print()

#Dimensiunea setului de date este:
sdf_dim(spark_features)

#-----------------------------------------------------------------
    ### Vizualizare detalii despre spark_stores dataframe:

#Dimensiunea setului de date este:
sdf_dim(spark_stores)
glimpse(spark_stores)

# Obtinerea valorilor unice din coloana Type
spark_stores %>%
  group_by(Type) %>%
  summarise(count = n()) %>%
  arrange(desc(count))%>%
  collect()

#-----------------------------------------------------------------
#Imbinarea seturilor de date pe coloane comune intr-un singur dataframe
#pentru ulterioarele analize.

#Am combinat seturile de date spark_sales si spark_stores pe baza 
#atributelor magazinului si setul de date rezultat este imbinat cu 
#setul de date spark_features pe baza atributelor Store, Date si IsHoliday.

spark_merge_df <- spark_sales %>%
  inner_join(spark_stores, by = "Store") %>%
  inner_join(spark_features, by = c("Store", "Date", "IsHoliday")) %>%
  sdf_register("merge_df")

#Acum avem merge_df salvat in Spark. 
src_tbls(sc)

glimpse(spark_merge_df)
sdf_dim(spark_merge_df)

#Convertim IsHoliday in variabila numerica cu 0 = False si 1 = True
#Cream si o noua variabila Type numeric unde A = 1, B=2, C = 3
spark_merge_df <- spark_merge_df %>%
  mutate(IsHoliday = as.integer(IsHoliday), 
         # Convertim direct variabila booleana in 0 sau 1
         Type_numeric = case_when(
           Type == "A" ~ 1,
           Type == "B" ~ 2,
           Type == "C" ~ 3 ),
         #Adăugarea coloanelor „An”, „Lună” si „Saptamana” in merge_df
         Year = year(Date),
         Month = month(Date), 
         WeekOfYear = weekofyear(Date) 
  )

glimpse(spark_merge_df)

#----------------------------------------------------------------------

#Colectam datele in R pentru a putea realiza graficele si alte analize.
#Au fost salvate dataframe-urile curatate in R pentru a le folosi ulterior 
#la incarcare mult mai usor.

#r_merge_df <- collect(spark_merge_df)
#r_features <- collect(spark_features)
#r_sales<- collect(spark_sales)
#r_stores<-collect(spark_stores)

#Salvare:
#save.image(file = "all_dataframes_proiect.RData")

#--------------------------------------------------------------------
                  ### Analiza exploratorie a datelor

#Pentru grafice vom folosi setul de date din local R deoarece aducerea
#datelor din dataframe spark cu functia collect(), ar dura prea mult la
#afisarea graficelor pentru ca setul de date este prea mare ducand astfel
#la un proces prea lent.

#1. Histograma vanzari saptamanale
#var 1 -------------------------------
spark_merge_df %>%
  collect() %>%
  ggplot(aes(x = Weekly_Sales)) +
  geom_histogram(bins = 30, fill = "red") +
  labs(title = "Weekly Sales Distribution") +
  scale_x_continuous(labels = scales::comma) +
  scale_y_continuous(labels = scales::comma)

#var 2 -------------------------------
# Selectarea coloanei Weekly_Sales din DataFrame-ul Spark
weekly_sales <- r_merge_df %>% 
  select(Weekly_Sales) %>% 
  collect()

# Crearea histogramei folosind pachetul ggplot2
ggplot(weekly_sales, aes(x = Weekly_Sales)) +
  geom_histogram(binwidth = 9000, fill = "red", color = "black", alpha = 0.7) +
  labs(title = "Histograma Vânzărilor Săptămânale",
       x = "Vânzări Săptămânale",
       y = "Frecvență") +
  theme_minimal()

#Calculam vanzarile saptamanale pentru fiecare tip de magazin
r_merge_df %>%
  group_by(Type) %>%
  summarise(total_sales = sum(Weekly_Sales)) %>%
  arrange(desc(total_sales)) %>%
  show()

#2. Skewness+kurtosis pentru variabila weekly_sales

#Crearea graficului densității
ggplot(r_merge_df %>% filter(!is.na(Weekly_Sales)), 
       aes(x = Weekly_Sales)) +
  geom_density(color = "white", fill = 'red', alpha = 0.5) +
  ggtitle("Graficul de densitate al vânzărilor săptămânale") +
  xlim(min(r_merge_df$Weekly_Sales), 200000) 
#INTEPRETARE:
#graficul indica o asimetrie la dreapta si o distributie puternic leptocurtica, 
#insa vom, calcula si indicatorii pentru valiudare	# calcul indicatori

# Calculul skewness si kurtosis pentru variabila Weekly_Sales
#var 1 ---------------
library(PerformanceAnalytics)
r_merge_df %>%
  summarise(
    skewness = PerformanceAnalytics::skewness(Weekly_Sales), 
    kurtosis = PerformanceAnalytics::kurtosis(Weekly_Sales) 
  )
#var 2 ---------------
#install.packages("moments")
library(moments)
skewness_value <- skewness(r_merge_df$Weekly_Sales)
kurtosis_value <- kurtosis(r_merge_df$Weekly_Sales)

print(paste("Skewness: ", skewness_value))
print(paste("Kurtosis: ", kurtosis_value)) 
#INTERPRETARE
#distributia are un skewness egal cu 3, ceea ce indica o asimetrie la dreapta,
#iar kurtosis=21 indica o distributie puternic leptocurtica.

#3. Popularitatea tipurilor de magazine:
library(plotly)
# Calculul numarului de aparitii pentru fiecare tip de magazin
type_counts <- r_merge_df %>%
  count(Type)
# Crearea pie chart-ului utilizând plotly
plot_ly(type_counts, labels = ~Type, values = ~n, type = 'pie',
        textposition = 'inside', textinfo = 'percent+label') %>%
  layout(title = "Popularity of Store Types")

#INTERPRETARE
#Magazinele de tip A sunt mai populare decat cele de tip B si C


#4.	Vanzarile medii - tip magazin
# Calcularea vânzărilor medii și colectarea rezultatelor
avg_weekly_sales <- spark_merge_df %>%
  group_by(Type) %>%
  summarise(AvgSales = mean(Weekly_Sales)) %>%
  collect()

# Crearea graficului folosind ggplot2
ggplot(avg_weekly_sales, aes(x = Type, y = AvgSales)) +
  geom_bar(stat = "identity", fill = "lightskyblue") +
  labs(title = "Vânzările medii pentru fiecare tip de magazin",
       x = "Tip de Magazin",
       y = "Vânzări Medii") +
  theme_minimal()
#INTERPRETARE
#Pe baza graficului nostru cu bare, se observa că magazinele de tip „A” 
#au cele mai mari vânzări în comparație cu celelalte două magazine.
#Acest lucru ne spune că există o relație directă între dimensiunea 
#magazinului și vânzările lor corespunzătoare. Magazinele de tip „B” au 
#locul al doilea ca marime, numărul de magazine precum și vânzările medii, 
#dovedind astfel această ipoteză.

#5.	Vânzările medii pentru fiecare departament:
# Calcularea vânzărilor medii pentru fiecare departament și aranjarea descrescătoare
departmentsales <- spark_merge_df %>%
  group_by(Dept) %>%
  summarise(AvgSales = mean(Weekly_Sales)) %>%
  arrange(desc(AvgSales)) %>%
  collect()

# Crearea graficului folosind ggplot2
ggplot(departmentsales, aes(x = AvgSales, y = factor(Dept))) +
  geom_bar(stat = "identity", fill = "#114D77") +
  labs(title = "Average Sales for Each Department", x = "Average Sales", y = "Department") +
  theme_minimal() +
  scale_x_continuous(breaks = seq(0, max(departmentsales$AvgSales), by = 10000)) +
  theme(axis.text.y = element_text(hjust = 1))
#INTERPRETARE 
#Din imagine, se observa ca diferite departamente au prezentat 
#niveluri diferite de vânzări medii.Departamentele cu numerele
#92,95,38,72,65 prezinta cel mai mare număr de vânzări medii.

#6. vanzarile medii pentru fiecare magazin
# Calcularea vânzărilor medii pentru fiecare magazin și aranjarea descrescătoare
store_sales <- spark_merge_df %>%
  group_by(Store) %>%
  summarise(AvgSales = mean(Weekly_Sales)) %>%
  arrange(desc(AvgSales)) %>%
  collect()

# Crearea graficului folosind ggplot2
ggplot(store_sales, aes(x = factor(Store, levels = Store), y = AvgSales)) +
  geom_bar(stat = "identity", fill = "coral", color = "black") +
  labs(title = "Average Sales for Each Store", x = "Stores", y = "Average Sales") +
  theme_minimal() +
  scale_x_discrete(labels = store_sales$Store) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
#INTERPRETARE
#Din graficul cu bare observam ca magazinel 20,4,14,13,2 au cele mai mari vânzări medii in toti cei trei ani.
#De asemenea, trebuie remarcat faptul că există o diferență foarte mare între 
#vânzările medii pentru fiecare dintre magazine; în timp ce unele magazine 
#înregistrează vânzări uriașe, altele lipsesc foarte mult în zonă. 
#Acest lucru ar putea depinde de factori precum tipurile de produse 
#vândute de magazin, locația geografică, temperatura, șomajul din 
#vecinătate etc. Un studiu suplimentar arată că toate aceste trei 
#magazine aparțin magazinului de tip A care adună cele mai mari 
#vânzări din toate cele trei tipuri de magazine.

#7.	Creare boxplot Type Size Store
merge_df_collected <- spark_merge_df %>%
  select(Type, Size) %>%
  collect()

# Crearea graficului folosind ggplot2
ggplot(merge_df_collected, aes(x = Type, y = Size, fill = Type)) +
  geom_boxplot(show.legend = FALSE, outlier.shape = NA) +
  labs(x = "Type", y = "Size", title = "Type-Size Relation") +
  theme_minimal() +
  theme(plot.title = element_text(hjust = 0.5)) +
  coord_cartesian(ylim = c(0, max(merge_df_collected$Size))) +
  coord_flip()
#INTERPRETARE
#Mărimea tipului de magazine este în concordanță cu vânzările, 
#așa cum era de așteptat. Magazinele de dimensiuni mai mari au 
#vânzări mai mari. Walmart clasifică magazinele în funcție de
#dimensiunile lor conform graficului. 
#După cea mai mică valoare de dimensiune a tipului A, 
#începe tipul B, apoi tipul C.

#8.	Vânzările medii lunare pentru fiecare an
monthly_sales <- spark_merge_df %>%
  filter(Year %in% 2010:2012) %>%
  group_by(Year, Month) %>%
  summarise(AvgSales = mean(Weekly_Sales)) %>%
  collect()

ggplot(monthly_sales, aes(x = as.factor(Month), y = AvgSales, fill = as.factor(Year))) +
  geom_bar(stat = "identity", position = "dodge") +
  labs(title = "Average Monthly Sales for Each Year",
       x = "Month",
       y = "Average Sales",
       fill = "Year") +
  theme_minimal()
#INTERPRETARE
#Observatii din grafic:
#1.Luna ianuarie a avut cele mai scăzute vânzări pentru anii 2011 și 2012, 
#în timp ce pentru 2010  nu au fost inregistrare vânzările săptămânale.
#2.Din Luna Februarie până în Octombrie, vânzările săptămânale rămân 
#aproape constante în jurul valorii de 15000 pentru cei 3 ani.
#3. Lunile Noiembrie și Decembrie au înregistrat cele mai mari vânzări 
#pentru 2010 și 2011, în timp ce pentru 2012 datele de vânzări 
#nu au fost furnizate.

#9.	Vânzări săptămânale medii - pe an
weekly_sales <- spark_merge_df %>%
  mutate(WeekOfYear = as.character(WeekOfYear)) %>%
  group_by(Year, WeekOfYear) %>%
  summarise(AvgSales = mean(Weekly_Sales, na.rm = TRUE), .groups = "drop") %>%
  collect()

# Crearea graficului
ggplot(weekly_sales, aes(x = WeekOfYear, y = AvgSales, color = Year, group = Year)) +
  geom_line() +
  labs(x = "Week of Year", y = "Sales", title = "Average Weekly Sales - Per Year") +
  scale_color_manual(values = c("2010" = "blue", "2011" = "red", "2012" = "green")) +
  theme_minimal() +
  theme(axis.text.x = element_text(size = 10), axis.text.y = element_text(size = 10),
        axis.title.x = element_text(size = 10, margin = margin(t = 20)),
        axis.title.y = element_text(size = 10, margin = margin(r = 20)),
        legend.text = element_text(size = 10), legend.title = element_blank(),
        legend.position = c(0, 1), legend.justification = c(0, 1))
#INTERPRETARE 
#Observatii din grafic:
#Săptămânal, în săptămâna sărbătorii de Ziua Recunoștinței 
#și cu o săptămână înainte de Crăciun, au fost cele mai mari 
#vânzări din anii 2010 și 2011.
#În 2012 săptămâna nr. 14 a înregistrat cele mai mari vânzări
#în comparație cu alte săptămâni ale anului, dar asta nu corespunde
#cu nicio sărbătoare sau vreun eveniment special.

#10. Tipul magazinului cu cele mai mari vânzări pentru fiecare tip de magazin
ggplot(r_merge_df, aes(x = Year, y = Weekly_Sales, fill = Type)) +
  geom_bar(stat = "identity", position = "dodge") +
  labs(title = "Weekly Sales by Year and Type",
       x = "Year",
       y = "Weekly Sales") +
  theme_minimal()
# INTERPRETARE
#observam faptul ca in 2010, cele mai multe vanzari au fost inregistrate de magazinul tip B, 
#urmat de magazinul tip A si n final C, aceasta structura menbtinandu se si in anul 2011

#in anul 2012 in schimb, desi structura se mentine, vanzarile magazinului de tip B au devenit aproape 
#egale cu cele ale magazinului de tip B , ambele au scazut ca volum fata de anii precedenti, iar cele
#ale magazinului de tip C au crescut fata de anii anteriori, insa ramane tot sub nivelul celor doua
#magazine de tip A si B

#11. 	Impactul dimensiunii magazinului asupra vânzărilor - (merge dar se incarca foarte greu) 
vanzari_type<-spark_merge_df %>%
  select(Weekly_Sales, Size, Type) %>%
  collect()

ggplot(vanzari_type, aes(x = Size, y = Weekly_Sales, color=Type)) +
  geom_point() +
  labs(title = "Impactul Mărimii Magazinului asupra Vânzărilor Săptămânale",
       x = "Mărimea Magazinului",
       y = "Vânzări Săptămânale",
       color = "Tipul Magazinului") +
  theme_minimal()

#INTERPRETARE
# Se știe din vizualizarea anterioară că tipul "A" este cel mai mare tip de magazin,
# urmat de tipul "B" și "C". 
# Câteva magazine de tip "B", așa cum este descris mai jos, înregistrează vânzări medii mai mari decât 
# magazinele de tip "A", ceea ce contravine ideii generale conform căreia cu cât dimensiunea 
# magazinului este mai mare, cu atât vânzările sunt mai mari. Dar, în general, magazinele de
# tip A prezintă în continuare un volum mare de vânzări, în timp ce magazinele de tip C
# prezintă un volum de vânzări semnificativ mai mic. În concluzie, vânzările cresc,
# în general, odată cu creșterea dimensiunii magazinului, cu câteva excepții minore.


#12. Impactul temperaturii asupra vânzărilor
ggplot(merge_df, aes(x = Temperature, y = Weekly_Sales, color=Type)) +
  geom_point() +
  labs(title = "Impactul temperaturii asupra Vânzărilor Săptămânale",
       x = "Temperatura medie",
       y = "Vânzări Săptămânale",
       color = "Tipul Magazinului") +
  theme_minimal()
#INTERPRETARE
# După cum se vede mai jos, cele mai mari vânzări au loc pentru majoritatea tipurilor de magazine
# între 40 și 80 de grade Fahrenheit, dovedind astfel ideea că vremea plăcută încurajează vânzări 
# mai mari. Vânzările sunt relativ mai scăzute pentru temperaturi foarte scăzute și foarte ridicate,
# dar par a fi suficient de ridicate pentru condiții climatice favorabile.

#13. Impactul șomajului asupra vânzărilor
ggplot(merge_df, aes(x = Unemployment, y = Weekly_Sales, color=Type)) +
  geom_point() +
  labs(title = "Impactul ratei somajului asupra Vânzărilor Săptămânale",
       x = "Rata somajului",
       y = "Vânzări Săptămânale",
       color = "Tipul Magazinului") +
  theme_minimal()
#INTERPRETARE
# Pentru tipurile de magazine date, se pare că există o scădere vizibilă a 
# vânzărilor atunci când indicele de șomaj este mai mare de 11 
# - Chiar și atunci când indicele de șomaj este mai mare de 11, nu există 
# nicio schimbare semnificativă în vânzările medii pentru magazinele de tip 
# C în comparație cu vânzările generale 
# - Se pare că există o scădere semnificativă a vânzărilor
# pentru tipurile de magazine A și B atunci când indicele de șomaj crește 
# - Cele mai mari vânzări înregistrate pentru tipurile de magazine A și B 
# apar în jurul indicelui de șomaj de 8-10; acest lucru oferă idei ambigue 
# cu privire la impactul șomajului asupra vânzărilor pentru fiecare dintre magazinele

#14. Impactul prețului_combustibilului asupra vânzărilor
ggplot(merge_df, aes(x = Fuel_Price, y = Weekly_Sales, color=Type)) +
  geom_point() +
  labs(title = "Impactul pretului combustibilului asupra Vânzărilor Săptămânale",
       x = "Pretul combustibilului",
       y = "Vânzări Săptămânale",
       color = "Tipul Magazinului") +
  theme_minimal()
#INTERPRETARE
# în timp ce pare să existe o scădere a vânzărilor atunci când prețul combustibilului este mai mare
# de 4,25 dolari, vânzările sunt mai mari atunci când prețul combustibilului variază între 2,75 și 
# 3,75 dolari. Unele dintre cele mai mari vânzări pentru tipurile de magazine A și B au loc în această 
# perioadă. Deși nu există un model clar care să dovedească acest lucru, unele observații susțin teoria 
# conform căreia prețurile mai mici ale carburanților încurajează creșterea vânzărilor


#15. Holiday vs non-holiday
# Calcularea vânzărilor medii pentru fiecare valoare 'IsHoliday'
sales_holiday <- spark_merge_df %>%
  group_by(IsHoliday) %>%
  summarise(mean_sales = mean(Weekly_Sales)) %>%
  collect()

# Numărul de apariții pentru fiecare valoare 'IsHoliday'
nr_holiday <- spark_merge_df %>%
  count(IsHoliday) %>%
  collect()

# Crearea graficului pentru vânzările medii în funcție de 'IsHoliday'
vanzari_sarbatori <- ggplot(sales_holiday, aes(x = IsHoliday, y = mean_sales, fill = IsHoliday)) +
  geom_bar(stat = "identity") +
  geom_text(aes(label = sprintf("%.2f", mean_sales)), vjust = -0.5, color = "black", size = 3) +
  labs(x = "IsHoliday?", y = "Mean Sales", title = "Holiday vs Non-Holiday Sales") +
  scale_fill_manual(values = c("FALSE" = "blue", "TRUE" = "red")) +
  theme_minimal()
#INTERPRETARE
# Deși datele de sărbători au reprezentat doar aproape 7 % din zilele din an, acestea au totuși 
# vânzări săptămânale mai mari decât restul anului la un loc (după cum se vede în imaginea de mai jos). Vânzările și 
# numărătorile "IsHoliday" = TRUE pot fi observate în figura 21 și evidențiază faptul că, deși numărul de date de 
# sărbători este mult mai mic decât cel al datelor normale din setul de date, acestea acumulează mai multe vânzări.


#16.Vânzările din săptămâna anului în funcție de tipul de magazin - DE VERIFICAT
merge_df <- spark_merge_df %>%
  mutate(Year = year(Date), Month = month(Date), WeekOfYear = week(Date)) %>%
  collect()

ggplot(merge_df, aes(x = WeekOfYear, y = Weekly_Sales, color=Type)) +
  geom_point() +
  labs(title = "Impactul saptamanii anului asupra Vânzărilor Săptămânale",
       x = "Saptamana anului",
       y = "Vânzări Săptămânale",
       color = "Tipul Magazinului") +
  theme_minimal()
#INTERPRETARE
# Cu acest grafic, este clar că, spre deosebire de tipul de magazin A și B, vânzările medii nu cresc neapărat pentru 
# tipul C la sfârșitul anului, în jurul Zilei Recunoștinței și al Crăciunului. De asemenea, arată că magazinele de tip 
# A au, de obicei, vânzări săptămânale mai mari în comparație cu celelalte două magazine, dovedind încă o dată că o 
# dimensiune mai mare a magazinului înseamnă vânzări mai mari


#----------------MATRICEA DE CORELATIE
numeric_columns <- select(r_merge_df, where(is.numeric))
correlation_matrix <- cor(numeric_columns)
library(corrplot)
# Afisarea plot-ului de corelație utilizând corrplot
corrplot(correlation_matrix, method = "color", type = "upper",
         addrect = 8, order = "FPC", tl.col = "black",
         tl.srt = 45, diag = TRUE, title = "Correlation Plot")



#-------------------------------------------------------------------------
      #### Selectia esantionului pentru construirea modelor ML ####
#--------------------------------------------------------------------------

 #Din setul de date initial, s-a realizat o selectie a datelor 
 #doar pentru vanzarile saptamanale din anul 2011.
 #De asemenea, pe baza studiului de analiza si corelatie a datelor exploratorii,
 #au fost eliminate caracteristicile care nu influenteaza 
 #in mod semnficativ si au o corelatie slaba cu variabila dependenta Weekly_Sales.

 #Caracteristicile eliminate sunt:
  #-> Cele 5 tipuri de reduceri (Markdown(1:5)) deoarece prezinta o corelatie slaba 
  #-> temperatura aerului, prețul combustibilului, tipul de magazin, CPI, rata șomajului, 
  #si data in care s-au înregistrat vânzările

s_mergedf_2011 <- spark_merge_df %>%
  filter(Year == 2011) %>%
  select(-Date, -Temperature, -Fuel_Price, -Type, -starts_with("MarkDown"), -CPI, -Month, -Unemployment) %>%
  sdf_register("s_mergedf_2011")

glimpse(s_mergedf_2011)
sdf_dim(s_mergedf_2011)
#Setul de date pe care il vom utiliza in crearea modelelor ML are 8 coloane si 152940 de observatii.


#-------------------------------------------------------------------------
       #### Impartirea setului de date: train(75%) si test(25%) ####
#--------------------------------------------------------------------------

### Pregatirea seturilor de date: train si test

model_partition_tbl <- s_mergedf_2011 %>%
  sdf_random_split(train = 0.75, test = 0.25, seed = 1234)

glimpse(model_partition_tbl)

# Creare referinte tabele si incarcarea datelor in Spark
s_mergedf2011_train <- sdf_register(model_partition_tbl$train, "s_mergedf2011_train")
s_mergedf2011_test <- sdf_register(model_partition_tbl$test, "s_mergedf2011_test")

sdf_dim(s_mergedf2011_train)
#[1] 114920      8
sdf_dim(s_mergedf2011_test)
#[1] 38020     8


##Creare formula
ml_formula <- formula(Weekly_Sales ~ .)

#-------------------------------------------------------------------------
         #### Creare modele fara validare incrucisata ####
#--------------------------------------------------------------------------

compute_metrics <- function(model, test_data, response_var) {
  # Calculam predictiile pe setul de testare
  predictions <- ml_predict(model, test_data)
  
  # Calculam radacina erorii medie patratica (RMSE)
  rmse <- ml_regression_evaluator(predictions, label_col = response_var, metric_name = "rmse")
  
  # Calculam R-squared
  r_squared <- ml_regression_evaluator(predictions, label_col = response_var, metric_name = "r2")
  
  # Afisam RMSE si R-squared
  return(list(RMSE = rmse, R_squared = r_squared))
}

      #### 1. Modelul de regresie liniara multiplu (MRLM) ####

model_lr <- ml_linear_regression(s_mergedf2011_train, formula = ml_formula)
summary(model_lr)
print(compute_metrics(model_lr, s_mergedf2011_test, "Weekly_Sales"))

        #### 2. Modelul Random Forest ####

model_rf <- ml_random_forest(s_mergedf2011_train, formula = ml_formula)
summary(model_rf)
print(compute_metrics(model_rf, s_mergedf2011_test, "Weekly_Sales"))

        #### 3. Gradient Boosted Tree ####

model_gbt <- ml_gradient_boosted_trees(s_mergedf2011_train, formula = ml_formula)
summary(model_gbt)
print(compute_metrics(model_gbt, s_mergedf2011_test, "Weekly_Sales"))
 
        #### 4. Decision Tree ####

# Model de regresie cu arbori de decizie
model_dt <- ml_decision_tree_regressor(s_mergedf2011_train, formula = ml_formula)
summary(model_dt)

#### Calculare metrici pentru fiecare model
metrics_lr <- compute_metrics(model_lr, s_mergedf2011_test, "Weekly_Sales")
metrics_rf <- compute_metrics(model_rf, s_mergedf2011_test, "Weekly_Sales")
metrics_gbt <- compute_metrics(model_gbt, s_mergedf2011_test, "Weekly_Sales")
metrics_dt <- compute_metrics(model_dt, s_mergedf2011_test, "Weekly_Sales")

###Afisare metrici pentru fiecare model sub forma de tabel
all_metrics <- bind_rows(
  list(metrics_lr, metrics_rf, metrics_gbt, metrics_dt), .id = "Model") %>%
  mutate(Model = c("MLR", 
                   "Random Forest", 
                   "Gradient Boosted Tree",
                   "Decision Tree")[as.integer(Model)])


#Plot comparatie metrici intre modele
all_metrics %>%
  tidyr::gather(metric, value, RMSE, R_squared) %>%
  ggplot(aes(x = Model, y = value, fill = metric)) +
  geom_bar(stat = "identity", position = "dodge") +
  facet_wrap(~ metric, scales = "free_y") +
  labs(x = "Model", y = "Value", fill = "Metric") +
  ggtitle("RMSE and R-squared for Each Model")


#Importanta variabilelor pentru modelele RF si GBT
importance_rf <- ml_tree_feature_importance(sc = sc, model = model_rf)
importance_gbt <- ml_tree_feature_importance(sc = sc, model = model_gbt)
importance <- rbind(importance_rf, importance_gbt)
# Adaugare numele fiecarui model in dataframe
importance$Model <- c(rep("Random Forest", nrow(importance_rf)), 
                      rep("Gradient Boosted Tree", nrow(importance_gbt)))

# Plot importanta variabilelor pentru modelele RF si GBT
ggplot(importance, aes(x = reorder(feature, importance), y = importance, fill = Model)) +
  geom_bar(stat = "identity", position = "dodge") +
  labs(x = "Variabile", y = "Importanta", fill = "Model") +
  ggtitle("Importanta variabilelor pentru Random Forest si Gradient Boosted Tree") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

# Importanta variabilelor pentru modelul de regresie cu arbori de decizie
importance_dt <- ml_tree_feature_importance(sc = sc, model = model_dt)

# Adaugare numele modelului in dataframe
importance_dt$Model <- "Decision Tree"

# Plot importanta variabilelor pentru modelul de regresie cu arbori de decizie
ggplot(importance_dt, aes(x = reorder(feature, importance), y = importance, fill = Model)) +
  geom_bar(stat = "identity", position = "dodge") +
  labs(x = "Variabile", y = "Importanta", fill = "Model") +
  ggtitle("Importanta variabilelor pentru Decision Tree") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))


#-------------------------------------------------------------------------
          #### Creare modele cu validare incrucisata ####
#--------------------------------------------------------------------------

#Creare functie pentru cross_validation models unde am reglat 
#hiperparametrii si am generat metricile de performanta pentru 
#fiecare folder in parte.
#Aceasta functie a fost utilizata pentru crearea modelelor de regresie

ml_regression_cv<- function(
    data,            # input spark dataframe
    response,        # Predicted column
    features = NULL, # character vector or else all the columns except response
    model_fun,       # the modelling function to use (unquoted)
    k = 5,           # number of folds
    param_grid = NULL # hyperparameter grid for tuning
) {
  
  # Crearea ponderilor pentru partitionarea datelor
  weights <- rep(1 / k, times = k)
  names(weights) <- paste0("fold", as.character(1:k))
  
  # Partitionarea datelor folosind ponderile
  data_cv <- sdf_random_split(data, weights = weights)
  
  # Obtinerea indicilor pentru diferitele seturi de antrenament
  K <- 1:k
  indices <- purrr::map(K, ~ K[-.x])
  
  # Crearea seturilor de antrenament prin combinarea partitiilor din data_cv
  data_splits <- purrr::map(indices, ~ sdf_bind_rows(data_cv[.x]))
  
  # Daca nu a fost specificat un vector de nume de caracteristici
  if (is.null(features)) {
    columns <- colnames(data_splits[[1]])
    features <- columns[columns != response]
  }
  
  # Reglare de hiperparametri
  fits <- purrr::map(data_splits, function(train_data) {
    if (!is.null(param_grid)) {
      best_model <- NULL
      best_metric <- Inf
      best_params <- NULL
      for (params in param_grid) {
        model <- do.call(model_fun, c(list(train_data, response = response, features = features), params))
        predictions <- ml_predict(model, train_data)
        metric <- ml_regression_evaluator(predictions, label_col = response, metric_name = "rmse")
        if (metric < best_metric) {
          best_metric <- metric
          best_model <- model
          best_params <- params
        }
      }
      return(list(model = best_model, params = best_params))
    } else {
      model <- model_fun(train_data, response = response, features = features)
      return(list(model = model, params = NULL))
    }
  })
  
  # Generarea predictiilor pentru fiecare fold
  preds <- purrr::map2(fits, K, ~ ml_predict(.x$model, data_cv[[.y]]))
  
  # Evaluarea modelelor folosind evaluatori de regresie
  evals <- purrr::map2_df(preds, fits, ~ {
    rmse <- ml_regression_evaluator(.x, label_col = response, metric_name = "rmse")
    r2 <- ml_regression_evaluator(.x, label_col = response, metric_name = "r2")
    params <- .y$params
    if (is.null(params)) params <- list(num_trees = NA, max_depth = NA)
    tibble(
      RMSE = rmse,
      R_squared = r2,
      num_trees = params$num_trees,
      max_depth = params$max_depth
    )
  })
  
  # Returnarea rezultatelor
  list(fits = fits, predictions = preds, evals = evals)
}

# Combinatii de hiperparametri pentru Random Forest 
param_grid_rf <- list(
  list(num_trees = 20, max_depth = 5),
  list(num_trees = 50, max_depth = 10)
)

# Validare incrucisata pentru Random Forest
rf_cv3 <- ml_regression_cv(
  s_mergedf2011_train,
  response = "Weekly_Sales",
  model_fun = ml_random_forest,
  k = 5,
  param_grid = param_grid_rf
)

# Combinatii de hiperparametri pentru Gradient Boosted Trees
param_grid_gbt <- list(
  list(max_iter = 20, max_depth = 5),
  list(max_iter = 50, max_depth = 10)
)

# Validare incrucisata pentru Gradient Boosted Trees
gbt_cv3 <- ml_regression_cv(
  s_mergedf2011_train,
  response = "Weekly_Sales",
  model_fun = ml_gradient_boosted_trees,
  k = 5,
  param_grid = param_grid_gbt
)

# Combinatii de hiperparametri pentru Decision Tree
param_grid_dt <- list(
  list(max_depth = 5),
  list(max_depth = 10)
)

# Validare incrucisata pentru Decision Tree Regression
dt_cv3 <- ml_regression_cv(
  s_mergedf2011_train,
  response = "Weekly_Sales",
  model_fun = ml_decision_tree_regressor,
  k = 5,
  param_grid = param_grid_dt
)

# Extragem evaluarile intr-un dataframe
rf_results <- rf_cv3$evals
gbt_results<- gbt_cv3$evals

# Combinam rezultatele intr-un singur tabel
all_results_mod_csv <- bind_rows(
  rf_results %>% mutate(Model = "RF", Fold = rep(1:5, each = nrow(rf_results) / 5)),
  gbt_results %>% mutate(Model = "GBT", Fold = rep(1:5, each = nrow(gbt_results) / 5))
)
#Pentru GBT, num_trees este echivalent cu max_iter si am facut un artificiu fiindca 
#nu am specificat in functia ml_regression_cv() sa specific si num_iter pentru afisare
#si mi-am dat seama dupa ce am rulat modelul care a durat cam mult pentru procesare
all_results_mod_csv <- all_results_mod_csv %>%
  mutate(num_trees = ifelse(Model == "GBT" & is.na(num_trees), 50, num_trees))


#Rezultatele extrase intr-un dataframe pentru DT:
dt_results <- dt_cv3$evals
all_results_mod_csv <- bind_rows(
  dt_results %>% mutate(Model = "DT", Fold = rep(1:5, each = nrow(dt_results) / 5))
)


#Plot comparatie metrici modele cu cross-valid pe cele 5 foldere
all_results_mod_csv %>%
  tidyr::gather(metric, value, RMSE, R_squared) %>%
  arrange(Model, Fold, value) %>%
  mutate(Model = factor(Model, levels = unique(Model))) %>%
  ggplot(aes(x = interaction(Model, Fold), y = value, fill = metric)) +
  geom_bar(stat = "identity", position = position_dodge()) +
  geom_text(aes(label = round(value, 2)), position = position_dodge(width = 0.9), vjust = -0.5, size = 3) +
  labs(x = "Model (Fold)", y = "Value", fill = "Metric") +
  ggtitle("RMSE si R-squared pentru fiecare model cu validare incrucisata") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  facet_wrap(~ metric, scales = "free_y", ncol = 2)

#Plot comparatie metrici pentru model DT:
all_results_mod_csv %>%
  tidyr::gather(metric, value, RMSE, R_squared) %>%
  arrange(Fold, value) %>%
  mutate(Model = "DT") %>%
  ggplot(aes(x = interaction(Model, Fold), y = value, fill = metric)) +
  geom_bar(stat = "identity", position = position_dodge()) +
  geom_text(aes(label = round(value, 2)), position = position_dodge(width = 0.9), vjust = -0.5, size = 3) +
  labs(x = "Model (Fold)", y = "Value", fill = "Metric") +
  ggtitle("RMSE si R-squared pentru Decision Tree cu validare incrucisata") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  facet_wrap(~ metric, scales = "free_y", ncol = 2)

# Calcularea importantei pentru Random Forest
import_rf_cv3 <- lapply(rf_cv3$fits, function(model_info) {
  model <- model_info$model
  ml_tree_feature_importance(sc = sc, model = model)
})

# Calcularea importantei pentru Gradient Boosted Tree
import_gbt_cv3 <- lapply(gbt_cv3$fits, function(model_info) {
  model <- model_info$model
  ml_tree_feature_importance(sc = sc, model = model)
})

# Combinarea rezultatelor pentru modelele RF si GBT:
import_all <- do.call(rbind, c(import_rf_cv3, import_gbt_cv3))

# Adaugare coloana pentru nume model
import_all$model <- factor(rep(c("Random Forest", "Gradient Boosted Tree"), each = nrow(import_all) / 2))

# Grafic importanta caracteristicilor pentru modele cu cross-validation
ggplot(import_all, aes(x = reorder(feature, importance), y = importance, fill = model)) +
  geom_bar(stat = "identity", position = "dodge") +
  labs(x = "Variabile", y = "Importanta", fill = "Model") +
  ggtitle("Importanta variabilelor pentru modelele RF si GBT cu validare incrucisata") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))


# Calcularea importantei caracteristicilor pentru Decision Tree
import_dt_cv3 <- lapply(dt_cv3$fits, function(model_info) {
  model <- model_info$model
  ml_tree_feature_importance(sc = sc, model = model)
})

# Combinarea rezultatelor pentru DT
importance_dt <- do.call(rbind, import_dt_cv3)

# Adaugare coloana pentru nume model
importance_dt$model <- "Decision Tree"

# Grafic importanta caracteristicilor pentru Decision Tree
ggplot(importance_dt, aes(x = reorder(feature, importance), y = importance, fill = model)) +
  geom_bar(stat = "identity", position = "dodge") +
  labs(x = "Variabile", y = "Importanta", fill = "Model") +
  ggtitle("Importanta variabilelor pentru Decision Tree cu validare incrucisata") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))





