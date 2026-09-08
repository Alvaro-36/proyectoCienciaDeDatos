import asyncio
import json
import random
import string
from playwright.async_api import async_playwright

TOTAL = 10
TABS = 10
MAX_REINTENTOS = 3

PREFIJOS = ["Here is your API key: ", "Your dedicated access key is: "]
URL = "https://www.alphavantage.co/support/#api-key"
OUTPUT = "apis_generadas.json"


def generar_email():
    chars = string.ascii_lowercase + string.digits
    return "".join(random.choices(chars, k=9)) + "@yopmail.com"


def extraer_api_key(texto):
    for pre in PREFIJOS:
        inicio = texto.find(pre)
        if inicio != -1:
            inicio += len(pre)
            fin = texto.find(".", inicio)
            return texto[inicio:fin].strip()
    return None


async def recargar(page):
    """Recarga real: goto al mismo #api-key no recarga (navegacion same-document)."""
    try:
        if "alphavantage" in (page.url or ""):
            await page.reload(wait_until="domcontentloaded")
        else:
            await page.goto(URL, wait_until="domcontentloaded")
    except Exception:
        # fallback forzado: pasar por about:blank para obligar carga completa
        await page.goto("about:blank")
        await page.goto(URL, wait_until="domcontentloaded")
    await page.wait_for_load_state("domcontentloaded")
    await page.wait_for_selector("#submit-btn", timeout=15000)


async def intentar_una(page, email):
    """Un intento completo: recargar, rellenar form, tocar #submit-btn, leer key."""
    for _ in range(MAX_REINTENTOS):
        try:
            await recargar(page)
            await page.select_option('#occupation-text', label="Student")
            await page.fill('#organization-text', "UTN")
            await page.fill('#email-text', email)
            await page.click('#submit-btn')  # boton ya existente en el codigo original
            for _ in range(60):
                texto = await page.text_content('#talk') or ""
                if "API key:" in texto or "access key is:" in texto:
                    key = extraer_api_key(texto)
                    if key:
                        return key
                await page.wait_for_timeout(150)
            # si no aparecio la key, el proximo reintento llama recargar() de nuevo
        except Exception:
            pass
    return None


async def worker(tab_id, page, cuota, apis, lock):
    """Flow repetitivo de una pestana: obtener key -> recargar -> repetir."""
    conseguidas = 0
    while conseguidas < cuota:
        email = generar_email()
        resultado = await intentar_una(page, email)
        if resultado:
            async with lock:
                apis.append(resultado)
                total = len(apis)
                print(f"[Tab {tab_id}] OK {email} -> {resultado} "
                      f"({total}/{TOTAL})")
                if total % 10 == 0:
                    with open(OUTPUT, "w") as f:
                        json.dump(apis, f, indent=2)
                    print(f"Progreso: {total} keys guardadas")
            conseguidas += 1
            # la recarga ocurre al inicio del siguiente intentar_una via goto
        else:
            print(f"[Tab {tab_id}] Fallo con {email}, reintentando con nuevo email...")


async def main_async():
    apis = []
    lock = asyncio.Lock()
    # Reparto 100 keys entre 4 pestanas (25 c/u)
    cuotas = [TOTAL // TABS] * TABS
    for i in range(TOTAL % TABS):
        cuotas[i] += 1

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        # Abrir 4 pestanas en simultaneo (misma ventana)
        pages = [await browser.new_page() for _ in range(TABS)]
        print(f"Abiertas {TABS} pestanas, cuotas por tab: {cuotas}")
        await asyncio.gather(*[
            worker(i + 1, pages[i], cuotas[i], apis, lock)
            for i in range(TABS)
        ])
        await browser.close()

    with open(OUTPUT, "w") as f:
        json.dump(apis, f, indent=2)

    print(f"\nTotal: {len(apis)}/{TOTAL} APIs en {OUTPUT}")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
