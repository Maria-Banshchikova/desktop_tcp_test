"""
Автотест для сценария обрыва TCP-соединения.
Проверяет поведение клиента при потере связи с сервером.
"""

import socket # создание клиентского TCP-сокета
import json # парсинг JSON-данных от сервера
import time # паузы между действиями в тестах
import pytest # декораторы и assert с детальными сообщениями

from server.signal_server import HOST, PORT, SIGNAL_COUNT # Импортируем константы из модуля сервера, чтобы клиент и тесты использовали те же значения


# ============================================================
# Вспомогательный класс: клиент, имитирующий поведение приложения
# ============================================================
class SignalClient: # Объявляем класс SignalClient — тестовый TCP-клиент
    """
    Минимальный TCP-клиент, имитирующий десктоп-приложение.
    Подключается к серверу, принимает сигналы, определяет обрыв.
    """

    def __init__(self, host: str = HOST, port: int = PORT, timeout: float = 2.0):
        self.host = host # Сохраняем адрес сервера в атрибут экземпляра
        self.port = port # Сохраняем порт 
        self.timeout = timeout # Сохраняем таймаут 
        self.sock = None # Инициализируем сокет как None (не подключён)
        self.connected = False # Флаг подключения, изначально False
        self.last_signals = [] # Список последних полученных сигналов, изначально пустой

    def connect(self) -> bool: # Метод подключения к серверу
        """Устанавливает TCP-соединение. Возвращает True при успехе."""
        try: # перехват ошибок подключения
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # Создаём TCP-сокет (IPv4, TCP)
            self.sock.settimeout(self.timeout) # Устанавливаем таймаут на операции с сокетом (recv, connect)
            self.sock.connect((self.host, self.port)) # Подключаемся к серверу по адресу (host, port) — кортеж
            self.connected = True # Устанавливаем флаг подключения в True
            return True
        except (ConnectionRefusedError, socket.timeout, OSError): # Перехват: ConnectionRefusedError — сервер недоступен; socket.timeout — превышен таймаут; OSError — прочие сетевые ошибки
            self.connected = False # Сбрасываем флаг подключения
            return False

    def receive_signals(self) -> list: # Метод приёма данных, аннотация возврата — list
        """
        Принимает одну порцию сигналов.
        Возвращает список сигналов или пустой список при ошибке/обрыве.
        """
        if not self.connected: # Проверка: если клиент не подключён 
            return []

        try:
            data = self.sock.recv(4096) # Читаем до 4096 байт из сокета; recv блокирует до прихода данных или таймаута
            if not data: # Если data пустой (b"") — сервер закрыл соединение со своей стороны
                # Сервер закрыл соединение — это обрыв
                self.connected = False # Сбрасываем флаг подключения
                return [] # Возвращаем пустой список — признак обрыва

            # Парсим JSON
            signals = json.loads(data.decode("utf-8").strip()) # decode("utf-8") — байты в строку; strip() — убирает пробелы и \n по краям; json.loads — парсит JSON в список словарей
            self.last_signals = signals # Сохраняем полученные сигналы в атрибут last_signals
            return signals

        except (socket.timeout, ConnectionResetError, BrokenPipeError, OSError): # Перехват: timeout — нет данных за 2 сек; ConnectionResetError — сброс; BrokenPipeError — запись в закрытый сокет; OSError — прочее
            self.connected = False # Сбрасываем флаг подключения
            return [] # Возвращаем пустой список — признак обрыва/ошибки

    def disconnect(self): # Метод закрытия соединения
        """Закрывает соединение."""
        if self.sock: # Проверяем, что сокет существует
            try: # закрытие может выбросить OSError если сокет уже закрыт
                self.sock.close() # Закрываем сокет, освобождаем ресурсы ОС
            except OSError: # Если сокет уже закрыт — игнорируем ошибку
                pass # пустой оператор, ничего не делает (заглушка)
        self.connected = False # Сбрасываем флаг подключения


# ============================================================
# Тест 1. Позитивный: успешное подключение и приём данных
# ============================================================
def test_connect_and_receive_signals(signal_server): # аргумент signal_server — фикстура из conftest.py 
    """
    Проверяем, что клиент подключается и получает все 10 сигналов.
    """
    client = SignalClient() # Создаём экземпляр тестового клиента (использует HOST, PORT по умолчанию)

    # Шаг 1: подключаемся
    assert client.connect() is True, "Клиент не смог подключиться к серверу" # assert проверяет истинность; строка после запятой — сообщение при провале
    assert client.connected is True, "Флаг connected не установлен" # Проверяем, что флаг connected установлен в True

    # Шаг 2: получаем данные
    signals = client.receive_signals() # Вызываем приём сигналов; результат сохраняем в переменную signals

    # Шаг 3: проверяем результат 
    # len() возвращает длину списка; == сравниваем с 10; если не равно — вывести сообщение
    assert len(signals) == SIGNAL_COUNT, (
        f"Ожидалось {SIGNAL_COUNT} сигналов, получено {len(signals)}"
    )

    # Проверяем структуру каждого сигнала
    for sig in signals: # Цикл по каждому словарю-сигналу в списке
        # Оператор in проверяет наличие ключа ".." в словаре
        assert "id" in sig, "В сигнале нет поля id"
        assert "name" in sig, "В сигнале нет поля name"
        assert "value" in sig, "В сигнале нет поля value"
        assert "quality" in sig, "В сигнале нет поля quality"
        assert "timestamp" in sig, "В сигнале нет поля timestamp"
        assert sig["quality"] in ("Good", "Bad", "Uncertain"), (
            f"Недопустимое качество: {sig['quality']}"
        )

    client.disconnect()


# ============================================================
# Тест 2. Негативный: обрыв связи (сервер выключен)
# ============================================================
def test_connection_break_detected(signal_server):
    """
    Проверяем, что клиент обнаруживает обрыв связи,
    когда сервер останавливается.
    """
    client = SignalClient() # Создаём тестовый клиент

    # Шаг 1: подключаемся и получаем первую порцию данных
    assert client.connect() is True, "Клиент не смог подключиться"
    signals_before = client.receive_signals() # Принимаем первую порцию сигналов
    assert len(signals_before) == SIGNAL_COUNT, "Первая порция сигналов не получена" # Проверяем, что получили 10 сигналов

    # Шаг 2: останавливаем сервер — имитируем обрыв связи
    signal_server.stop() # Останавливаем сервер через фикстуру (метод stop класса SignalServer)
    time.sleep(0.5)  # даём серверу время закрыть сокет

    # Шаг 3: пытаемся получить следующую порцию
    signals_after = client.receive_signals() # Пауза 0.5 сек — даём серверу время закрыть сокет и освободить порт

    # Шаг 4: проверяем, что клиент обнаружил обрыв
    assert signals_after == [], ( # Проверяем, что получен пустой список (признак обрыва); == сравнение значения
        "Клиент не обнаружил обрыв связи: получены данные после остановки сервера"
    )
    assert client.connected is False, ( # Проверяем, что флаг connected сброшен в False
        "Флаг connected не сброшен после обрыва связи"
    )

    client.disconnect() # Закрываем сокет клиента (cleanup)


# ============================================================
# Тест 3. Позитивный: повторное подключение после обрыва
# ============================================================
def test_reconnect_after_break(signal_server):
    """
    Проверяем, что клиент может переподключиться после обрыва связи.
    """
    client = SignalClient() # Создаём тестовый клиент

    # Шаг 1: подключаемся и получаем данные
    assert client.connect() is True, "Первое подключение не удалось"
    assert len(client.receive_signals()) == SIGNAL_COUNT # Сразу в assert вызываем receive_signals и проверяем длину результата

    # Шаг 2: обрываем связь
    signal_server.stop() # Останавливаем сервер — имитация обрыва
    time.sleep(0.5) # Пауза для гарантированного закрытия сокета сервером
    client.receive_signals()  # должен вернуть []
    assert client.connected is False, "Клиент не обнаружил обрыв" # Проверяем, что клиент зафиксировал обрыв

    # Шаг 3: запускаем сервер заново
    signal_server.start() # Перезапускаем сервер через фикстуру (метод start класса SignalServer)
    time.sleep(0.5) # Пауза, чтобы сервер успел занять порт

    # Шаг 4: переподключаемся
    assert client.connect() is True, "Повторное подключение не удалось" # Проверяем, что клиент успешно переподключился

    # Шаг 5: получаем данные
    signals = client.receive_signals() # Принимаем сигналы после переподключения
    assert len(signals) == SIGNAL_COUNT, ( # Проверяем, что получили 10 сигналов; скобки для многострочного сообщения
        f"После переподключения ожидалось {SIGNAL_COUNT} сигналов, "
        f"получено {len(signals)}"
    )

    client.disconnect() # Закрываем соединение клиента (cleanup)