import pandas as pd
import numpy as np
import re

class PreProcessor:

    def __init__(self, csv_file_path):
        self.__df = pd.read_csv(csv_file_path, index_col= 0)
        self.__del_space()
        self.__df = self.__df.replace(r'^\s*$', np.nan, regex=True) # Заменяем пустые строки в данных на тип NaN
        # self.__preprocessing_size_col()
        self.__preprocessing_extra_size_col()
        del self.__df["Доп. размер"]
        self.__union_price_cols()
        self.__df = self.__df.replace(r'^\s*$', np.nan, regex=True)

    @property
    def df(self):
        return self.__df
    
    def __del_space(self):
        for col in self.__df.columns: 
            self.__df[col] = self.__df[col].str.strip()
    
    def __preprocessing_extra_size_col(self):
        col_name = 'Доп. размер'
        df = self.__df
        
        df[col_name] = df[col_name].astype(str).str.lower().str.strip()
        df[col_name] = df[col_name].replace(['', 'nan', 'н.д', 'нд', 'н/д', 'с н/д', 'с ост.'], np.nan)

        def extract_all_data(text):
            if pd.isna(text):
                return pd.Series([np.nan, np.nan, np.nan, np.nan], index=['Минимальная_длина', 'Максимальная_длина', 'Упаковка', 'Примечание'])

            original_text = text

            packaging = np.nan
            if 'бухты' in text:
                packaging = 'бухты'
                text = text.replace('бухты', '').strip()
            elif 'размотка' in text:
                packaging = 'размотка'
                text = text.replace('размотка', '').strip()
            elif 'мотки' in text or 'розетты' in text:
                packaging = 'мотки/розетты'
                text = text.replace('мотки', '').replace('розетты', '').strip()
            
            length_primary = np.nan
            length_max = np.nan
            notes = text

            # Вариант 1: Диапазон "число-число" (e.g., "2-6", "3.4-3.7")
            range_match = re.search(r'(\d+\.?\d*)\s*-\s*(\d+\.?\d*)', text)
            if range_match:
                vals = sorted([float(range_match.group(1)), float(range_match.group(2))])
                length_primary, length_max = vals[0], vals[1]
                # Очищаем текст от найденного диапазона для примечаний
                notes = re.sub(r'\d+\.?\d*\s*-\s*\d+\.?\d*', '', text).strip()

            # Вариант 2: Размеры через "х" (e.g., "1.5х10", "1000х1000")
            elif 'х' in text:
                # Эти размеры не являются линейными, поэтому оставляем их в примечаниях
                # а поля длин оставляем пустыми.
                notes = original_text 
                
            # Вариант 3: Префикс "до" (e.g., "до 12")
            elif text.startswith('до '):
                numbers = re.findall(r'\b\d+(?:\.\d+)?\b', text)
                if numbers:
                    length_max = float(numbers[0])
                notes = text
            
            # Вариант 4: Стандартные числа (одиночные или списки)
            else:
                numbers_str = re.findall(r'\b\d+(?:\.\d+)?\b', text)
                if numbers_str:
                    numeric_values = sorted([float(n) for n in numbers_str])
                    if len(numeric_values) == 1:
                        length_primary = length_max = numeric_values[0]
                    elif len(numeric_values) > 1:
                        length_primary = numeric_values[0]
                        length_max = numeric_values[-1]
                    
                    notes = re.sub(r'[\d\.\s,]+', '', text).strip()

            if not notes or notes.isspace():
                notes = np.nan

            return pd.Series([length_primary, length_max, packaging, notes], index=['Минимальная_длина', 'Максимальная_длина', 'Упаковка', 'Примечание'])

        extracted_data = df[col_name].apply(extract_all_data)
        
        for col in extracted_data.columns:
            self.__df[col] = extracted_data[col]

    def __union_price_cols(self):
        
        cols_with_price = [col for col in self.__df.columns if re.search(r"Цена, ", col)]
        price_df = self.__df[cols_with_price]
        
        stacked_prices = price_df.stack()
        
        # Если цен вообще не нашлось, выходим, чтобы не было ошибок
        if stacked_prices.empty:
            self.__df['Цена'] = np.nan
            self.__df['Категория_цены'] = np.nan
            self.__df['Условие_цены'] = np.nan
            self.__df['Звоните'] = False
            self.__df.drop(columns=cols_with_price, inplace=True, errors='ignore')
            return
    
        final_prices = stacked_prices.reset_index()
        final_prices.columns = ['original_index', 'Категория_цены', 'temp_price']
    
        final_prices = final_prices.drop_duplicates(subset='original_index', keep='last')
        
        final_prices = final_prices.set_index('original_index')
        
        self.__df = self.__df.join(final_prices)
    
        PRICECOL = 'Цена'
        CALLCOL = 'Звоните'
        
        is_call = self.__df['temp_price'].str.lower() == 'звоните'
        self.__df[CALLCOL] = is_call.fillna(False)
        
        self.__df.loc[self.__df[CALLCOL], 'temp_price'] = np.nan
    
        pattern = r'^(?P<Цена>[\d\s.,]+)(?:\s+(?P<Условие_цены>.+))?$'
        extracted_data = self.__df['temp_price'].str.extract(pattern)
        
        self.__df[PRICECOL] = extracted_data['Цена'] if not extracted_data.empty else np.nan
        self.__df['Условие_цены'] = extracted_data['Условие_цены'] if not extracted_data.empty else np.nan
        
        self.__df[PRICECOL] = self.__df[PRICECOL].apply(
            lambda x: float(str(x).replace(" ", "").replace(",", ".")) if pd.notna(x) else np.nan
        )
    
        self.__df['Категория_цены'] = self.__df['Категория_цены'].str.replace('Цена, ', '', regex=False)
    
        self.__df.drop(columns=cols_with_price + ['temp_price'], inplace=True, errors='ignore')
    
    