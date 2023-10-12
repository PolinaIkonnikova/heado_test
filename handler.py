import argparse
import re
import csv
import logging
import sys
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List

logger = logging.getLogger()


def is_desimal_form(val: str) -> bool:
    return bool(re.fullmatch(r"\-?(\d+\.\d+|\d+)", val))


def is_valid_decimal_10_4(val: str, min_num=-9999.9999, max_num=9999.9999) -> bool:
    return is_desimal_form(val) and min_num < float(val) < max_num


def is_valid_decimal_12_4(val: str) -> bool:
    return is_valid_decimal_10_4(val, min_num=-99999.9999, max_num=99999.9999)


def is_valid_decimal_13_4(val: str) -> bool:
    return is_valid_decimal_10_4(val, min_num=-999999.9999, max_num=999999.9999)


def is_valid_decimal_4_2(val: str) -> bool:
    return is_valid_decimal_10_4(val, min_num=-99.99, max_num=99.99)


def is_valid_index(val: str) -> bool:
    return bool(re.fullmatch(r"[0-9a-zA_Z]{1,40}", val))


class PriceRunner:

    def __init__(self, csv_file: Path):

        self.csv_file = csv_file
        self.endpoint = "https://api.heado.ru/management/<Auth_Key>"
        self.method_name = "inventoryUpdateBatch"
        self.id = "1251506945251332"
        self.fields = ["name", "categories", "price", "price_ext_id", "vat", "unit_type", "unit_ratio"]
        self.required_fields = {"name", "price"}
        self.rebuild_field = "categories"

    @property
    def json_file(self) -> str:
        """ Возвращает имя json файла """
        return self.csv_file.as_posix().replace(self.csv_file.suffix, '.json')

    @staticmethod
    def _is_valid_name(val: str) -> bool:
        return len(val) <= 200

    @staticmethod
    def _valid_categs(val: str) -> List[str]:
        if val:
            separate_categories = [cat.split("\n") for cat in val.split(' ')]
            return list(set([cat.replace(',', '') for cat in sum(separate_categories, [])]))
        return []

    @staticmethod
    def _is_valid_enum(val: str) -> bool:
        try:
            return int(val) in {1, 10, 11, 12, 20, 21, 30, 31}
        except ValueError:
            return False

    def _rebuild_fields(self, var: str):
        return self._valid_categs(var)

    @property
    def _get_field_validators(self) -> Dict:

        return {
            "name": self._is_valid_name,
            "price": is_valid_decimal_10_4,
            "price_ext_id": is_valid_index,
            "vat": is_valid_decimal_4_2,
            "unit_type": self._is_valid_enum,
            "unit_ratio": is_valid_decimal_10_4
        }

    def _rows_filter(self, array) -> List[Dict]:
        """ Фильтрует строки в массиве данных."""

        items = []

        for ind, row in enumerate(array):
            ind += 1

            if len(row) != len(self.fields):
                logger.warning(f" cтрока {ind}: Неверное количество полей: {row}")
                continue

            try:
                row_data = {}
                for name, val in zip(self.fields, row):
                    if name == self.rebuild_field:
                        new_val = self._rebuild_fields(val)
                        if not new_val:
                            logger.warning(f' строка {ind} : Не удалось преобразовать {name}: "{val}"')
                            continue

                        row_data[name] = new_val
                        continue

                    if self._get_field_validators[name](val):
                        row_data[name] = val
                    else:
                        if val != "":
                            logger.warning(f' строка {ind} : Неверное значение {name}: "{val}"')
                        elif val == "":
                            if name in self.required_fields:
                                logger.warning(f' строка {ind} : Отсутствует обязательное значение {name}')

            except Exception as err:
                logger.warning(f"Неизвестная ошибка в строке {ind}: {row}\n{err.args[0]}!")
                continue

            items.append(row_data)
        return items

    def _get_csv_data(self):
        if not self.csv_file.exists():
            logger.error(f"Файл {self.csv_file} не найден.")
            raise FileExistsError

        with open(self.csv_file, encoding="utf-8") as f:
            reader = csv.reader(f, delimiter=',')
            return self._rows_filter(reader)

    def csv_to_json(self):
        """ Переводит данные из csv в спецификацию json-rpc """

        request_dict = {"id": self.id, "method": self.method_name, "jsonrpc": "2.0"}
        items = self._get_csv_data()
        request_dict["params"] = {"items": items}

        with open(self.json_file, "w", encoding='utf-8') as file:
            json.dump(request_dict, file, indent=4, ensure_ascii=False)

    def send_cmd_request(self) -> str:
        return f"curl -X POST -H Content-Type: application/json -d @{self.json_file} {self.endpoint}"


class InventoryRunner(PriceRunner):

    def __init__(self, csv_file: Path):
        super().__init__(csv_file)
        # TODO можно дописать актуальный эндпойнт
        self.endpoint = "https://api.heado.ru/management/<Auth_Key>"
        self.method_name = "price.updateBatch"
        self.id = "1251506945251332"
        self.fields = ["store_ext_id", "price_ext_id", "snapshot_datetime", "in_matrix", "qty", "sell_price",
                       "prime_cost", "min_stock_level", "stock_in_days", "in_transit"]
        self.required_fields = {"store_ext_id", "price_ext_id", "snapshot_datetime", "qty"}
        self.rebuild_field = "snapshot_datetime"

    @staticmethod
    def _valid_data(val: str) -> str:
        try:
            data = val.split(" ")
            if len(data[1]) == 2:
                val += ":00:00"
            elif len(data[1]) == 5:
                val += ":00"
            date_object = datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
            return date_object.strftime("%d-%m-%Y %H:%M:%S")
        except (ValueError, IndexError):
            return ""

    @staticmethod
    def _is_valid_matr(val: str) -> bool:
        return val in {'true', 'false', '0', '1'}

    @staticmethod
    def _is_valid_insign_dec(val: str) -> bool:
        return is_desimal_form(val) and 0 <= float(val) < 999999.9999

    @staticmethod
    def _is_valid_smallint(val: str) -> bool:
        return re.fullmatch(r"[0-9]+", val) and 0 < int(val) < 32767

    @property
    def _get_field_validators(self) -> Dict:
        return {
            "store_ext_id": is_valid_index,
            "in_matrix": self._is_valid_matr,
            "price_ext_id": is_valid_index,
            "qty": is_valid_decimal_13_4,
            "sell_price": is_valid_decimal_12_4,
            "prime_cost": is_valid_decimal_12_4,
            "min_stock_level": self._is_valid_insign_dec,
            "stock_in_days": self._is_valid_smallint,
            "in_transit": is_valid_decimal_10_4
        }

    def _rebuild_fields(self, val: str) -> str:
        return self._valid_data(val)


def get_arg_parser():
    """ Описание скрипта и получение аргументов """

    parser = argparse.ArgumentParser(description="Скрипт для извлечения данных из csv формата, валидации"
                                                 "и сохранения их в файл в виде json-rpc спецификации. Есть "
                                                 "дополнительная возможность для дальнейшей отправки POST запросом.")

    parser.add_argument('runner', type=str, help='Выбор обработчика данных. '
                                                 'Доступные обработчики: price, inventory')
    parser.add_argument('csv_file', type=str, help='Путь к cvs файлу с данными')
    parser.add_argument('--ignore', help="Если нужно исключить обработку столбцов в документе, введите "
                                         "названия столбцов через запятую без пробелов.")
    parser.add_argument('-s', '--send', action="store_true", help='показывает вариант POST запроса на '
                        'эндпойнт.')
    return parser


def log_settings():
    """ Настройка логов """

    log_format = "%(asctime)s %(levelname)s: %(message)s"
    formatter = logging.Formatter(log_format, datefmt="%d-%m-%Y %H:%M:%S")
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)


def main():
    runners = {'price': PriceRunner, 'inventory': InventoryRunner}
    log_settings()
    parser = get_arg_parser()
    args = parser.parse_args()

    try:
        csv_file = Path(args.csv_file)

        if csv_file.suffix != '.csv':
            logger.error(f"Скрипт обрабатывает только csv файлы")
            sys.exit(1)

        runner = runners[args.runner](csv_file)

        if args.ignore:
            exclude_rows = args.ignore.split(",")
            if not set(exclude_rows).issubset(set(runner.fields)):
                logger.error(f"Ошибка в названии заданных исключенных полей")
                sys.exit(1)
            if set(exclude_rows).intersection(runner.required_fields):
                logger.error(f"В исключенных полях заданы обязательные поля")
                sys.exit(1)
            runner.fields = [item for item in runner.fields if item not in exclude_rows]
        runner.csv_to_json()

        if args.send:
            runner.send_cmd_request()
    except KeyError:
        logger.error(f"Обработчик указан неверно. Доступные обработчики price и inventory.")
        sys.exit(1)
    except FileExistsError:
        sys.exit(1)
    except Exception as err:
        logger.exception(f"Произошла непредвиденная ошибка!\n{str(err)}")
        sys.exit(1)


if __name__ == '__main__':
    main()
