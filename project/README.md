# Итоговый проект по курсу «Инженерия Искусственного Интеллекта»

Проект посвящен построению сервиса раннего прогнозирования риска отмены бронирования отеля. На основе табличных признаков бронирования модель оценивает вероятность отмены, а итоговое решение доступно как через ноутбуки для анализа, так и через `FastAPI`-сервис для инференса.

## Паспорт проекта

- Название: `Hotel Booking Cancellation Risk Service`
- Автор: `Онищук Никита Игоревич`
- Группа: `ИКБО-07-22`
- Контакт: `@gorkinpot`

## Структура проекта

- `data/` - датасет `hotel_bookings.csv` и описание данных
- `notebooks/` - ноутбуки для EDA и сравнения моделей
- `src/` - основной код проекта
- `configs/` - конфиги, `.env.example`, пример запроса
- `tests/` - тесты
- `artifacts/` - сохраненные артефакты модели
- `report.md` - отчет по проекту
- `self-checklist.md` - чеклист самопроверки
- `requirements.txt` - зависимости проекта
- `Dockerfile` - контейнеризация сервиса

## Датасет

Используется открытый датасет **Hotel Booking Demand**.

- основной файл: `data/hotel_bookings.csv`
- целевая переменная: `is_canceled`

## Установка окружения

```powershell
cd project
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## Работа через ноутбуки

Ноутбуки находятся в папке `notebooks/`.

- `01_eda.ipynb` - разведочный анализ данных
- `02_baselines.ipynb` - baseline-модели и сравнение метрик

Рекомендуемый порядок:

1. Активировать `.venv`
2. Открыть `01_eda.ipynb`
3. Выбрать интерпретатор `project/.venv/Scripts/python.exe`
4. Выполнить `Run All`
5. Затем открыть `02_baselines.ipynb` и тоже выполнить `Run All`

Если ядро не появляется в `VS Code`, нужно вручную выбрать интерпретатор:

- `Python: Select Interpreter`
- `F:\MIREA\ArtificialIntelligenceEngineering\project\.venv\Scripts\python.exe`

## Обучение модели

```powershell
cd project
.\.venv\Scripts\Activate.ps1
python -m src.train --config configs/train.json
```

После обучения в `artifacts/` сохраняются:

- `model.joblib` - лучшая модель
- `preprocessor.joblib` - препроцессор
- `metrics.json` - метрики моделей
- `metadata.json` - информация о выбранной модели
- `feature_columns.json` - список признаков

Текущий результат на полном датасете:

- лучшая модель: `mlp_classifier`
- `ROC-AUC = 0.8917`
- `F1 = 0.7349`
- `PR-AUC = 0.8637`

## Запуск API-сервиса

```powershell
cd project
.\.venv\Scripts\Activate.ps1
uvicorn src.service.app:app --host 127.0.0.1 --port 8000
```

После запуска доступны:

- `GET /health`
- `POST /predict`
- Swagger UI: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)

Пример запроса из PowerShell:

```powershell
$body = @{
  lead_time = 120
  adr = 95.5
  adults = 2
  children = 0
  babies = 0
  previous_cancellations = 1
  previous_bookings_not_canceled = 0
  booking_changes = 1
  days_in_waiting_list = 0
  required_car_parking_spaces = 0
  total_of_special_requests = 2
  stays_in_weekend_nights = 1
  stays_in_week_nights = 3
  hotel = "Resort Hotel"
  meal = "BB"
  market_segment = "Online TA"
  distribution_channel = "TA/TO"
  deposit_type = "No Deposit"
  customer_type = "Transient"
  reserved_room_type = "A"
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://127.0.0.1:8000/predict" `
  -Method Post `
  -ContentType "application/json" `
  -Body $body
```

## Запуск тестов

```powershell
cd project
.\.venv\Scripts\Activate.ps1
pytest tests
```

Текущее состояние:

- `4/4` тестов проходят успешно

## Запуск через Docker

```powershell
cd project
docker build -t aie-project .
docker run -p 8000:8000 aie-project
```

## Что показать на защите

1. Структуру проекта: `data/`, `notebooks/`, `src/`, `tests/`, `artifacts/`
2. `01_eda.ipynb` с анализом целевой переменной и ключевых признаков
3. `02_baselines.ipynb` со сравнением моделей
4. `report.md` с постановкой задачи, данными и выводами
5. Запуск `uvicorn` и один запрос к `/predict`

Ключевой тезис защиты: на полном датасете были сравнены `LogisticRegression`, `RandomForest` и `MLPClassifier`; лучшей по `ROC-AUC`, `F1` и `PR-AUC` оказалась `MLPClassifier`, поэтому именно она используется в итоговом сервисе.

## Ограничения

- интерпретация выполнена в легком формате, без `SHAP`
- сервис рассчитан на предсказание по одной записи
- в проекте нет БД, авторизации и продвинутого мониторинга

## Дальнейшее развитие

- добавить калибровку вероятностей
- расширить интерпретацию предсказаний
- добавить мониторинг и хранение истории запросов
- усилить feature engineering

## Оценка проекта

Проект закрывает ключевые требования курса к сквозному мини-проекту:

- сервис запускается по `README` и использует реальную обученную модель
- есть EDA, эксперименты и сравнение нескольких моделей по метрикам
- финальная модель обоснована в `report.md`
- есть базовая наблюдаемость через логи и `/health`
- есть конфиги, `.env.example`, тесты, Dockerfile и сохраненные артефакты

По чеклисту `self-checklist.md` проект закрывает все основные пункты и соответствует уровню сильной работы на `4-5`, а при аккуратной демонстрации и защите может претендовать на `5`.
