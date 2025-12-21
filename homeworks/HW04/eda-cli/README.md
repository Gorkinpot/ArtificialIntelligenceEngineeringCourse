# S04 – eda_cli: HTTP-сервис качества датасетов (FastAPI)

Расширенная версия проекта `eda-cli` из Семинара 03.

К существующему CLI-приложению для EDA добавлен **HTTP-сервис на FastAPI** с эндпоинтами `/health`, `/quality`, `/quality-from-csv` и `/quality-flags-from-csv`.  
Используется в рамках Семинара 04 курса «Инженерия ИИ».

---

## Связь с S03

Проект в S04 основан на том же пакете `eda_cli`, что и в S03:

- сохраняется структура `src/eda_cli/` и CLI-команда `eda-cli`;
- добавлен модуль `api.py` с FastAPI-приложением;
- в зависимости добавлены `fastapi` и `uvicorn[standard]`.

Цель S04 – показать, как поверх уже написанного EDA-ядра поднять простой HTTP-сервис.

---

## Требования

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) установлен в систему
- Браузер (для Swagger UI `/docs`) или любой HTTP-клиент:
  - `curl` / HTTP-клиент в IDE / Postman / Hoppscotch и т.п.

---

## Инициализация проекта

В корне проекта (каталог S04/eda-cli):

```bash
uv sync
```

Команда:

- создаст виртуальное окружение `.venv`;
- установит зависимости из `pyproject.toml` (включая FastAPI и Uvicorn);
- установит сам проект `eda-cli` в окружение.

---

## Запуск CLI (как в S03)

CLI остаётся доступным и в S04.

### Краткий обзор

```bash
uv run eda-cli overview data/example.csv
```

Параметры:

- `--sep` - разделитель (по умолчанию `,`);
- `--encoding` - кодировка (по умолчанию `utf-8`).

### Полный EDA-отчёт

```bash
uv run eda-cli report data/example.csv --out-dir reports
```

В результате в каталоге `reports/` появятся:

- `report.md` - основной отчёт в Markdown;
- `summary.csv` - таблица по колонкам;
- `missing.csv` - пропуски по колонкам;
- `correlation.csv` - корреляционная матрица (если есть числовые признаки);
- `top_categories/*.csv` - top-k категорий по строковым признакам;
- `hist_*.png` - гистограммы числовых колонок;
- `missing_matrix.png` - визуализация пропусков;
- `correlation_heatmap.png` - тепловая карта корреляций.

### Просмотр первых N строк

```bash
uv run eda-cli head data/example.csv --n 10
```

Выводит первые N строк датасета. Параметры:

- `--n` - количество строк для вывода (по умолчанию 5);
- `--sep` - разделитель (по умолчанию `,`);
- `--encoding` - кодировка (по умолчанию `utf-8`).

---

## Запуск HTTP-сервиса

HTTP-сервис реализован в модуле `eda_cli.api` на FastAPI.

### Запуск Uvicorn

```bash
uv run uvicorn eda_cli.api:app --reload --port 8000
```

Пояснения:

- `eda_cli.api:app` - путь до объекта FastAPI `app` в модуле `eda_cli.api`;
- `--reload` - автоматический перезапуск сервера при изменении кода (удобно для разработки);
- `--port 8000` - порт сервиса (можно поменять при необходимости).

После запуска сервис будет доступен по адресу:

```text
http://127.0.0.1:8000
```

---

## Эндпоинты сервиса

### 1. `GET /health`

Простейший health-check.

**Запрос:**

```http
GET /health
```

**Ожидаемый ответ `200 OK` (JSON):**

```json
{
  "status": "ok",
  "service": "dataset-quality",
  "version": "0.2.0"
}
```

Пример проверки через `curl`:

```bash
curl http://127.0.0.1:8000/health
```

---

### 2. Swagger UI: `GET /docs`

Интерфейс документации и тестирования API:

```text
http://127.0.0.1:8000/docs
```

Через `/docs` можно:

- вызывать `GET /health`;
- вызывать `POST /quality` (форма для JSON);
- вызывать `POST /quality-from-csv` (форма для загрузки файла);
- вызывать `POST /quality-flags-from-csv` (форма для загрузки файла).

---

### 3. `POST /quality` – заглушка по агрегированным признакам

Эндпоинт принимает **агрегированные признаки датасета** (размеры, доля пропусков и т.п.) и возвращает эвристическую оценку качества.

**Пример запроса:**

```http
POST /quality
Content-Type: application/json
```

Тело:

```json
{
  "n_rows": 10000,
  "n_cols": 12,
  "max_missing_share": 0.15,
  "numeric_cols": 8,
  "categorical_cols": 4
}
```

**Пример ответа `200 OK`:**

```json
{
  "ok_for_model": true,
  "quality_score": 0.8,
  "message": "Данных достаточно, модель можно обучать (по текущим эвристикам).",
  "latency_ms": 3.2,
  "flags": {
    "too_few_rows": false,
    "too_many_columns": false,
    "too_many_missing": false,
    "no_numeric_columns": false,
    "no_categorical_columns": false
  },
  "dataset_shape": {
    "n_rows": 10000,
    "n_cols": 12
  }
}
```

**Пример вызова через `curl`:**

```bash
curl -X POST "http://127.0.0.1:8000/quality" \
  -H "Content-Type: application/json" \
  -d '{"n_rows": 10000, "n_cols": 12, "max_missing_share": 0.15, "numeric_cols": 8, "categorical_cols": 4}'
```

---

### 4. `POST /quality-from-csv` – оценка качества по CSV-файлу

Эндпоинт принимает CSV-файл, внутри:

- читает его в `pandas.DataFrame`;
- вызывает функции из `eda_cli.core`:

  - `summarize_dataset`,
  - `missing_table`,
  - `compute_quality_flags`;
- возвращает оценку качества датасета в том же формате, что `/quality`.

**Запрос:**

```http
POST /quality-from-csv
Content-Type: multipart/form-data
file: <CSV-файл>
```

Через Swagger:

- в `/docs` открыть `POST /quality-from-csv`,
- нажать `Try it out`,
- выбрать файл (например, `data/example.csv`),
- нажать `Execute`.

**Пример вызова через `curl` (Linux/macOS/WSL):**

```bash
curl -X POST "http://127.0.0.1:8000/quality-from-csv" \
  -F "file=@data/example.csv"
```

Ответ будет содержать:

- `ok_for_model` - результат по эвристикам;
- `quality_score` - интегральный скор качества;
- `flags` - булевы флаги из `compute_quality_flags`;
- `dataset_shape` - реальные размеры датасета (`n_rows`, `n_cols`);
- `latency_ms` - время обработки запроса.

---

### 5. `POST /quality-flags-from-csv` – полный набор флагов качества

Эндпоинт принимает CSV-файл и возвращает **полный набор булевых флагов качества данных**, включая новые эвристики из HW03.

**Запрос:**

```http
POST /quality-flags-from-csv
Content-Type: multipart/form-data
file: <CSV-файл>
```

**Пример ответа `200 OK`:**

```json
{
  "flags": {
    "too_few_rows": false,
    "too_many_columns": false,
    "too_many_missing": false,
    "has_constant_columns": false,
    "has_high_cardinality_categoricals": true,
    "high_duplicate_values_ratio": false,
    "has_many_zero_values": true
  }
}
```

**Новые флаги из HW03:**

- `has_constant_columns` - наличие константных колонок (все значения одинаковые);
- `has_high_cardinality_categoricals` - категориальные признаки с высокой кардинальностью (>50% от числа строк или >50 уникальных значений);
- `high_duplicate_values_ratio` - высокое соотношение дубликатов (>70% значений повторяются);
- `has_many_zero_values` - много нулевых значений в числовых колонках (>50% нулей).

**Пример вызова через `curl`:**

```bash
curl -X POST "http://127.0.0.1:8000/quality-flags-from-csv" \
  -F "file=@data/example.csv"
```

---

## Эвристики качества данных

Проект использует набор эвристик для оценки качества данных:

### Базовые эвристики

- `too_few_rows` - слишком мало строк (< 100);
- `too_many_columns` - слишком много столбцов (> 100);
- `too_many_missing` - слишком много пропусков (> 50% в какой-либо колонке).

### Расширенные эвристики (HW03)

- `has_constant_columns` - константные колонки (все значения одинаковые);
- `has_high_cardinality_categoricals` - высокая кардинальность категориальных признаков;
- `high_duplicate_values_ratio` - высокое соотношение дубликатов;
- `has_many_zero_values` - много нулевых значений в числовых колонках.

Интегральная оценка `quality_score` вычисляется с учётом всех флагов и находится в диапазоне [0, 1].

---

## Логирование

Сервис использует **структурированное JSON-логирование**. Логи записываются в:

- Файл: `logs/api.log`;
- Стандартный вывод (stdout).

Каждая запись лога содержит:

- `endpoint` - путь эндпоинта;
- `status` - статус обработки (success/error);
- `latency_ms` - время обработки в миллисекундах;
- `ok_for_model` - результат оценки качества (если применимо);
- `n_rows`, `n_cols` - размеры датасета (если применимо);
- `timestamp` - временная метка (ISO 8601);
- `request_id` - уникальный идентификатор запроса (UUID).

Пример записи лога:

```json
{
  "endpoint": "/quality-from-csv",
  "status": "success",
  "latency_ms": 45.2,
  "ok_for_model": true,
  "n_rows": 36,
  "n_cols": 14,
  "timestamp": "2024-01-15T10:30:45.123Z",
  "request_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

---

## Структура проекта (упрощённо)

```text
S04/
  eda-cli/
    pyproject.toml
    README.md                # этот файл
    src/
      eda_cli/
        __init__.py
        core.py              # EDA-логика, эвристики качества
        viz.py               # визуализации
        cli.py               # CLI (overview/report/head)
        api.py               # HTTP-сервис (FastAPI)
    tests/
      test_core.py           # тесты ядра
    data/
      example.csv            # учебный CSV для экспериментов
    logs/
      api.log                # логи HTTP-сервиса (JSON)
```

---

## Тесты

Запуск тестов (как и в S03):

```bash
uv run pytest -q
```

Рекомендуется перед любыми изменениями в логике качества данных и API:

1. Запустить тесты `pytest`;
2. Проверить работу CLI (`uv run eda-cli overview ...`, `uv run eda-cli report ...`);
3. Проверить работу HTTP-сервиса (`uv run uvicorn ...`, затем `/health`, `/quality`, `/quality-from-csv` и `/quality-flags-from-csv` через `/docs` или HTTP-клиент).

---

## Примеры использования API

### Через Python (requests)

```python
import requests

# Health check
response = requests.get("http://127.0.0.1:8000/health")
print(response.json())

# Оценка качества из CSV
with open("data/example.csv", "rb") as f:
    response = requests.post(
        "http://127.0.0.1:8000/quality-from-csv",
        files={"file": f}
    )
    print(response.json())

# Полный набор флагов качества
with open("data/example.csv", "rb") as f:
    response = requests.post(
        "http://127.0.0.1:8000/quality-flags-from-csv",
        files={"file": f}
    )
    print(response.json())
```
