import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression


def plot_region_trend(csv_path: str, region_name: str):
    # читаем файл
    df = pd.read_csv(csv_path)

    # проверяем, что регион есть в таблице
    if region_name not in df['Субъект РФ'].values:
        raise ValueError(f"Регион '{region_name}' не найден в столбце 'Субъект РФ'")

    # строка с нужным регионом
    row = df[df['Субъект РФ'] == region_name].iloc[0]

    # названия месяцев (все столбцы, кроме первого)
    months = df.columns[1:]
    # значения медианной стоимости по месяцам
    values = row[1:].astype(float).to_numpy()

    # базовый месяц — январь 2021
    base_value = values[0]
    if base_value == 0:
        raise ValueError("Базовое значение равно нулю, нельзя перевести в проценты")

    # переводим всё в проценты от базового месяца
    values_pct = values / base_value * 100.0

    # линейная регрессия по индексам месяцев
    X = np.arange(len(months)).reshape(-1, 1)
    model = LinearRegression()
    model.fit(X, values_pct)
    y_pred = model.predict(X)

    # построение графика
    plt.figure(figsize=(18, 5))

    # реальная динамика
    plt.plot(months, values_pct, marker='o', label='Фактические значения')

    # линия регрессии
    plt.plot(months, y_pred, linestyle='--', label='Линейная регрессия')

    # горизонтальная линия 100% (базовый месяц)
    plt.axhline(100, color='gray', linestyle='dashed', linewidth=1)

    plt.xticks(rotation=60, ha='right')
    plt.xlabel('Месяц')
    plt.ylabel('% от базового месяца (январь 2021)')
    plt.title(f'Изменение медианной стоимости — {region_name}\n(100% = январь 2021)')
    plt.grid(axis='y', linestyle=':', linewidth=0.7)
    plt.tight_layout()
    plt.legend()

    plt.show()


if __name__ == "__main__":
    csv_path = "median_2021-2024.csv"   # путь к твоему файлу
    region = input("Введите название региона: ").strip()
    plot_region_trend(csv_path, region)
