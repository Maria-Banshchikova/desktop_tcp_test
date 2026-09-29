"""
Симулятор TCP-сервера.
Слушает порт 2001, отправляет 10 сигналов в формате JSON-строк,
разделённых символом новой строки.
"""

import socket  # низкоуровневый сетевой интерфейс (TCP/UDP)
import threading # потоки выполнения (запуск сервера в фоне)
import time    # паузы (sleep), временные метки
import json    # сериализация/десериализация данных в JSON
import math    # матем. функции (sin и др.)
import random  # генерация случайных чисел


HOST = "127.0.0.1"  # Константа: IP-адрес localhost (только локальные подключения)
PORT = 2001         # Константа: TCP-порт, который слушает сервер
SIGNAL_COUNT = 10   # Константа: количество сигналов в одной посылке

# Объявляем функцию с параметром tick (счётчик итераций) и аннотацией возврата list
def generate_signals(tick: int) -> list:
    """
    Генерирует список из 10 сигналов.
    Часть сигналов — синусоида, часть — случайные значения.
    """
    signals = []
    for signal_id in range(1, SIGNAL_COUNT + 1):
        if signal_id % 2 == 0:
            # Синусоида для чётных ID
            value = round(math.sin(tick * 0.1 + signal_id) * 100, 2) # math.sin возвращает синус; умножаем на 100 для масштаба; round(..., 2) округляет до 2 знаков
        else:
            # Случайное значение для нечётных ID
            value = round(random.uniform(0, 100), 2) # random.uniform(0, 100) — случайное число с плавающей точкой от 0 до 100; round до 2 знаков

        signals.append({     # Добавляем словарь (один сигнал) в список signals
            "id": signal_id, # Ключ "id" = номер сигнала (1–10)
            "name": f"Signal_{signal_id}",
            "value": value,
            "quality": "Good",
            "timestamp": time.time(), # Ключ "timestamp" = текущее время в секундах (Unix-эпоха, float)
        })
    return signals

# Объявляем класс SignalServer — TCP-сервер для тестов
class SignalServer:
    """TCP-сервер, который можно запускать и останавливать из тестов."""

    def __init__(self, host: str = HOST, port: int = PORT): # Конструктор класса с параметрами по умолчанию (HOST, PORT)
        self.host = host
        self.port = port
        self._sock = None     # Инициализируем сокет как None (нижнее подчёркивание = внутренний атрибут по соглашению)
        self._thread = None   # Инициализируем поток как None
        self._running = False # Флаг работы сервера, изначально False (сервер не запущен)

    def start(self): # Метод запуска сервера
        """Запускает сервер в отдельном потоке."""
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)   # Создаём TCP-сокет: AF_INET = IPv4, SOCK_STREAM = TCP
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # Разрешаем повторное использование адреса (чтобы избежать "Address already in use" при перезапуске)
        self._sock.bind((self.host, self.port))  # Привязываем сокет к адресу (host, port) — кортеж передаётся как один аргумент
        self._sock.listen(1)  # Переводим сокет в режим прослушивания; 1 = размер очереди ожидающих подключений
        self._running = True  # Устанавливаем флаг работы в True
        self._thread = threading.Thread(target=self._serve, daemon=True) # Создаём поток: target = функция, которую поток будет выполнять; daemon=True = поток завершится с основной программой
        self._thread.start() # Запускаем поток (вызывает self._serve в фоновом режиме)
        print(f"[SERVER] Запущен на {self.host}:{self.port}")

    def _serve(self): # Внутренний метод (приватный по соглашению) — главная цикл приёма подключений
        """Принимает клиента и отправляет данные, пока работает."""
        while self._running: # Цикл работает, пока флаг _running = True
            try: # Блок try — перехват исключений при accept
                conn, addr = self._sock.accept() # accept() блокирует поток до подключения клиента; возвращает кортеж (сокет соединения, адрес клиента)
                print(f"[SERVER] Подключён клиент: {addr}")
                self._handle_client(conn) # Передаём соединение в метод обработки клиента
            except OSError:  # Если сокет закрыт (при stop()) — accept() выбросит OSError
                break  # Выходим из цикла while — сервер останавливается

    def _handle_client(self, conn: socket.socket): # Внутренний метод обработки клиента; параметр conn — сокет подключения
        """Отправляет сигналы клиенту каждые 500 мс."""
        tick = 0 # Счётчик итераций — передаётся в generate_signals для изменения синусоиды
        try:
            while self._running: # Внутренний цикл отправки, работает пока сервер активен
                signals = generate_signals(tick) # Генерируем список из 10 сигналов для текущего тика
                # json.dumps преобразует список словарей в JSON-строку; + "\n" добавляет разделитель строк
                payload = json.dumps(signals) + "\n"
                conn.sendall(payload.encode("utf-8")) # encode("utf-8") переводит строку в байты; sendall отправляет все байты гарантированно
                tick += 1
                time.sleep(0.5)
        except (BrokenPipeError, ConnectionResetError): # Перехват: BrokenPipeError — запись в закрытый сокет; ConnectionResetError — сброс соединения клиентом
            print("[SERVER] Клиент отключился")
        finally:
            conn.close() # Закрываем соединение с клиентом, освобождаем ресурсы
    # Метод остановки сервера
    def stop(self):
        """Останавливает сервер."""
        self._running = False # Сбрасываем флаг работы — циклы while в _serve и _handle_client остановятся
        if self._sock:  # Проверяем, что сокет существует (не None)
            self._sock.close() # Закрываем сокет — accept() в _serve выбросит OSError и выйдет из цикла
        if self._thread: # Проверяем, что поток существует
            self._thread.join(timeout=2) # Ждём завершения потока не более 2 секунд (timeout чтобы не зависнуть навсегда)
        print("[SERVER] Остановлен")