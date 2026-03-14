# FX Curves and Quoting Conventions

This document outlines the conventions and potential architectural improvements regarding FX spot rates, quoting conventions, and FX curves within `py-flow`.

## Current State

Currently, `py-flow` codebase utilizes the convention:
`USD/JPY = 100`

This convention accurately reflects how the pairing is spoken out loud (e.g., "USD JPY is trading at a 100") and mirrors common usage on financial media platforms like Reuters or Bloomberg. 

## The Swap Market Convention

While colloquial, the current convention is less common in institutions with a strong swap trading heritage. In modern institutional pricing engines, this pair is typically written without the slash:
`USDJPY = 100`

When a slash is employed, it denotes a strict mathematical fraction where the symbols represent the currencies being exchanged. Thus, it is written the other way around:
`JPY/USD = 100`

This translates mathematically to:
`overCcy / underCcy = quoteCcy / baseCcy = 100`

This strict fractional representation (`JPY/USD`) is inherently more "mathematical" because it explicitly describes the amount of `quoteCcy` (JPY) one can purchase for exactly `1.0` of the `baseCcy` (USD). 

## Proposed Changes / To-Do

Migrating to the strict mathematical notation at this stage of the project would be relatively easy and highly beneficial for long-term maintainability to avoid pricing confusion inside cross-currency (`XCCY`) swaps and FX forward interpolations.

We should consider renaming these pairs and introducing a standardized namespace for spot tracking. Possible naming conventions could include:

- `FX_JPY/USD_SPOT` (mathematical fractional convention)
- `FX_USDJPY_SPOT` (common market-maker colloquial convention)

Making this change early establishes a rigorous mathematical foundation for when FX volatility surfaces and complex multi-currency triangulations are introduced.

## FX Curve Helper (Proposed)

Next steps could include adding an FX Curve helper capable of getting an FX Forward to any date, for any over/under pair. 

This component would:
1. Handle quote conventions that traditionally prefer > 1 ratios.
2. Formally track settlement spot days that vary by country holidays.
3. Automatically find the relevant published `FX_*_SPOT` and compute the forward using the fitted generic discount curves `IR_<over>_DISC_USD` and `IR_<under>_DISC_USD`.
4. Expose an `FXToday` value needed to actively net NPV figures spanning different currencies (which, confusingly, can deviate from quoted spot FX schedules).
