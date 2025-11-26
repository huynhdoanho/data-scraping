import logging
import os
from dotenv import load_dotenv
import requests
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

# Bật logging để theo dõi lỗi
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", 
    level=logging.WARNING
)

load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHANNEL_CHAT_ID")
print(TOKEN)

# Hàm xử lý lệnh /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(chat_id=update.effective_chat.id, text="Chào bạn! Tôi là một bot được tạo bởi HuynhDoanHo. Hãy gửi tin nhắn cho tôi!")

# Hàm xử lý tin nhắn văn bản và phản hồi lại
async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(chat_id=update.effective_chat.id, text=update.message.text)

# Hàm gửi tin nhắn thủ công sử dụng requests
def send_msg(chat_id, msg):
    requests.get(f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={msg}")
    return True

def main():
    # Tạo đối tượng Application
    application = Application.builder().token(TOKEN).build()

    # Tạo các trình xử lý (handler) cho lệnh và tin nhắn
    start_handler = CommandHandler('start', start)
    echo_handler = MessageHandler(filters.TEXT & (~filters.COMMAND), echo)

    # Đăng ký các handler với application
    application.add_handler(start_handler)
    application.add_handler(echo_handler)

    # Chạy bot cho đến khi bạn nhấn Ctrl-C
    application.run_polling()

if __name__ == '__main__':
    print("Bot is started.")
    msg = "Bot is started. \n HuynhDoanHo" 
    send_msg(CHAT_ID, msg)