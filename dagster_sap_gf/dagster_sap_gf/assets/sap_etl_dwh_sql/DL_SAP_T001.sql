SET XACT_ABORT ON;
DECLARE @filasactualizadas int = 0;
DECLARE @resultado nvarchar(500);

BEGIN
	BEGIN TRY --TABLA T001
		SET @resultado = 'paCargarDL_SAP_REPLICA_DatosMaestros.Iniciado T001'

		EXEC DL_paEscBitacora @resultado
			, 'Iniciado'
			, 'T001'

		BEGIN TRANSACTION;

		DROP TABLE
		IF EXISTS [DL_FARINTER].[dbo].[DL_SAP_T001] --PARA REFRESCAR TABLA COMPLETA

        IF NOT EXISTS (
                SELECT *
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = N'dbo'
                    AND TABLE_NAME = N'DL_SAP_T001'
                )
        BEGIN
            SELECT A.[BUKRS] COLLATE DATABASE_DEFAULT AS [BUKRS] -- X-Sociedad-Check: -Datatype:CHAR-Len:(4,0)
            , A.[BUTXT] COLLATE DATABASE_DEFAULT AS [BUTXT] --  -Denominación de la sociedad o empresa-Check: -Datatype:CHAR-Len:(25,0)
            , A.[ORT01] COLLATE DATABASE_DEFAULT AS [ORT01] --  -Población-Check: -Datatype:CHAR-Len:(25,0)
            , A.[LAND1] COLLATE DATABASE_DEFAULT AS [LAND1] --  -Clave de país-Check:T005-Datatype:CHAR-Len:(3,0)
            , A.[WAERS] COLLATE DATABASE_DEFAULT AS [WAERS] --  -Clave de moneda-Check:TCURC-Datatype:CUKY-Len:(5,0)
            , A.[SPRAS] COLLATE DATABASE_DEFAULT AS [SPRAS] --  -Clave de idioma-Check:T002-Datatype:LANG-Len:(1,0)
            , A.[KTOPL] COLLATE DATABASE_DEFAULT AS [KTOPL] --  -Plan de cuentas-Check:T004-Datatype:CHAR-Len:(4,0)
            , A.[WAABW] COLLATE DATABASE_DEFAULT AS [WAABW] --  -Desviación máxima del T/C en porcentaje-Check: -Datatype:NUMC-Len:(2,0)
            , A.[PERIV] COLLATE DATABASE_DEFAULT AS [PERIV] --  -Variante de ejercicio-Check:T009-Datatype:CHAR-Len:(2,0)
            , A.[KOKFI] COLLATE DATABASE_DEFAULT AS [KOKFI] --  -Indicador de asignación-Check: -Datatype:CHAR-Len:(1,0)
            , A.[RCOMP] COLLATE DATABASE_DEFAULT AS [RCOMP] --  -Sociedad GL-Check:T880-Datatype:CHAR-Len:(6,0)
            , A.[ADRNR] COLLATE DATABASE_DEFAULT AS [ADRNR] --  -Dirección-Check:*-Datatype:CHAR-Len:(10,0)
            , A.[STCEG] COLLATE DATABASE_DEFAULT AS [STCEG] --  -Número de identificación fiscal comunitario-Check: -Datatype:CHAR-Len:(20,0)
            , A.[FIKRS] COLLATE DATABASE_DEFAULT AS [FIKRS] --  -Entidad CP-Check:FM01-Datatype:CHAR-Len:(4,0)
            , A.[XFMCO] COLLATE DATABASE_DEFAULT AS [XFMCO] --  -Indicador: ¿Gestión de caja de proyectos activo?-Check: -Datatype:CHAR-Len:(1,0)
            , A.[XFMCB] COLLATE DATABASE_DEFAULT AS [XFMCB] --  -Indicador: Gestión de fondos activa-Check: -Datatype:CHAR-Len:(1,0)
            , A.[XFMCA] COLLATE DATABASE_DEFAULT AS [XFMCA] --  -Activar actualización en control presupuestario-Check: -Datatype:CHAR-Len:(1,0)
            , A.[TXJCD] COLLATE DATABASE_DEFAULT AS [TXJCD] --  -Cód.emplazamiento fiscal: Emplazamiento p.cálculo impuestos-Check:*-Datatype:CHAR-Len:(15,0)
            , A.[FMHRDATE] COLLATE DATABASE_DEFAULT AS [FMHRDATE] --  -Centro gestor contabilizable en HR de-Check: -Datatype:DATS-Len:(8,0)
            , A.[BUVAR] COLLATE DATABASE_DEFAULT AS [BUVAR] --  -Variante de sociedad (dynpro)-Check: -Datatype:CHAR-Len:(1,0)
            , A.[FDBUK] COLLATE DATABASE_DEFAULT AS [FDBUK] --  -Sociedad para la información de tesorería-Check:T001-Datatype:CHAR-Len:(4,0)
            , A.[XFDIS] COLLATE DATABASE_DEFAULT AS [XFDIS] --  -Indicador: Info de tesorería activa-Check: -Datatype:CHAR-Len:(1,0)
            , A.[XVALV] COLLATE DATABASE_DEFAULT AS [XVALV] --  -Indicador: ¿Proponer fecha CPU como fecha valor?-Check: -Datatype:CHAR-Len:(1,0)
            , A.[XSKFN] COLLATE DATABASE_DEFAULT AS [XSKFN] --  -Indicador: El importe base para DPP es el valor neto-Check: -Datatype:CHAR-Len:(1,0)
            , A.[KKBER] COLLATE DATABASE_DEFAULT AS [KKBER] --  -Área de control de créditos-Check:T014-Datatype:CHAR-Len:(4,0)
            , A.[XMWSN] COLLATE DATABASE_DEFAULT AS [XMWSN] --  -Indicador: ¿Valor neto como base imponible para impuesto?-Check: -Datatype:CHAR-Len:(1,0)
            , A.[MREGL] COLLATE DATABASE_DEFAULT AS [MREGL] --  -Normas para el traspaso de datos de cta.tipo a cta.mayor-Check:T004R-Datatype:CHAR-Len:(4,0)
            , A.[XGSBE] COLLATE DATABASE_DEFAULT AS [XGSBE] --  -Indicador: Balances a nivel de división-Check: -Datatype:CHAR-Len:(1,0)
            , A.[XGJRV] COLLATE DATABASE_DEFAULT AS [XGJRV] --  -Indicador: Proponer ejercicio-Check: -Datatype:CHAR-Len:(1,0)
            , A.[XKDFT] COLLATE DATABASE_DEFAULT AS [XKDFT] --  -Indicador: ¿Contabilizar riesgo trasl.en diferencias t/c?-Check: -Datatype:CHAR-Len:(1,0)
            , A.[XPROD] COLLATE DATABASE_DEFAULT AS [XPROD] --  -Indicador: Sociedad productiva-Check: -Datatype:CHAR-Len:(1,0)
            , A.[XEINK] COLLATE DATABASE_DEFAULT AS [XEINK] --  -Indicador: ¿Gestión de cuenta de compras activa?-Check: -Datatype:CHAR-Len:(1,0)
            , A.[XJVAA] COLLATE DATABASE_DEFAULT AS [XJVAA] --  -Indicador: Contabilidad JV activa-Check: -Datatype:CHAR-Len:(1,0)
            , A.[XVVWA] COLLATE DATABASE_DEFAULT AS [XVVWA] --  -Indicador: gestión de patrimonios activa-Check: -Datatype:CHAR-Len:(1,0)
            , A.[XSLTA] COLLATE DATABASE_DEFAULT AS [XSLTA] --  -Indicador:  Sin dif. de cambio en compensac. en moneda local-Check: -Datatype:CHAR-Len:(1,0)
            , A.[XFDMM] COLLATE DATABASE_DEFAULT AS [XFDMM] --  -Indicador: Actualización de MM activa en tesorería-Check: -Datatype:CHAR-Len:(1,0)
            , A.[XFDSD] COLLATE DATABASE_DEFAULT AS [XFDSD] --  -Indicador: Actualización de SD activa en tesorería-Check: -Datatype:CHAR-Len:(1,0)
            , A.[XEXTB] COLLATE DATABASE_DEFAULT AS [XEXTB] --  -Indicador: Sociedad situada en otro sistema-Check: -Datatype:CHAR-Len:(1,0)
            , A.[EBUKR] COLLATE DATABASE_DEFAULT AS [EBUKR] --  -Clave original de la sociedad-Check: -Datatype:CHAR-Len:(4,0)
            , A.[KTOP2] COLLATE DATABASE_DEFAULT AS [KTOP2] --  -Plan de cuentas según la legislación del país-Check:T004-Datatype:CHAR-Len:(4,0)
            , A.[UMKRS] COLLATE DATABASE_DEFAULT AS [UMKRS] --  -Sociedad IVA-Check:T007F-Datatype:CHAR-Len:(4,0)
            , A.[BUKRS_GLOB] COLLATE DATABASE_DEFAULT AS [BUKRS_GLOB] --  -Sociedad unívoca global-Check:T001O-Datatype:CHAR-Len:(6,0)
            , A.[FSTVA] COLLATE DATABASE_DEFAULT AS [FSTVA] --  -Variante de status de campos-Check:T004V-Datatype:CHAR-Len:(4,0)
            , A.[OPVAR] COLLATE DATABASE_DEFAULT AS [OPVAR] --  -Variante de los periodos contables-Check:T010O-Datatype:CHAR-Len:(4,0)
            , A.[XCOVR] COLLATE DATABASE_DEFAULT AS [XCOVR] --  -Indicador: Solicitud de garantía activa-Check: -Datatype:CHAR-Len:(1,0)
            , A.[TXKRS] COLLATE DATABASE_DEFAULT AS [TXKRS] --  -Conversión de cambio en pos. IVA-Check: -Datatype:CHAR-Len:(1,0)
            , A.[WFVAR] COLLATE DATABASE_DEFAULT AS [WFVAR] --  -Variante workflow-Check:VBWF01-Datatype:CHAR-Len:(4,0)
            , A.[XBBBF] COLLATE DATABASE_DEFAULT AS [XBBBF] --  -Verif. de autorización de cuentas de mayor en gestión stocks-Check: -Datatype:CHAR-Len:(1,0)
            , A.[XBBBE] COLLATE DATABASE_DEFAULT AS [XBBBE] --  -Verif. de autoriz. de cuentas de mayor en pedido/plan-entr.-Check: -Datatype:CHAR-Len:(1,0)
            , A.[XBBBA] COLLATE DATABASE_DEFAULT AS [XBBBA] --  -Verificación de autorización de cuentas de mayor en SolPed.-Check: -Datatype:CHAR-Len:(1,0)
            , A.[XBBKO] COLLATE DATABASE_DEFAULT AS [XBBKO] --  -Verif. de autorización de cuentas de mayor en ped. abiertos-Check: -Datatype:CHAR-Len:(1,0)
            , A.[XSTDT] COLLATE DATABASE_DEFAULT AS [XSTDT] --  -Indicador: Fecha de doc. como base para cálculo del IVA-Check: -Datatype:CHAR-Len:(1,0)
            , A.[MWSKV] COLLATE DATABASE_DEFAULT AS [MWSKV] --  -Indicador de impuesto de op. exenta IVA soportado-Check:*-Datatype:CHAR-Len:(2,0)
            , A.[MWSKA] COLLATE DATABASE_DEFAULT AS [MWSKA] --  -Indicador de impuestos de op. exenta IVA repercutido-Check:*-Datatype:CHAR-Len:(2,0)
            , A.[IMPDA] COLLATE DATABASE_DEFAULT AS [IMPDA] --  -Comercio exterior: Control dat.import.en pedidos compras MM-Check: -Datatype:CHAR-Len:(1,0)
            , A.[XNEGP] COLLATE DATABASE_DEFAULT AS [XNEGP] --  -Indicador: contab.negativas permitidas-Check: -Datatype:CHAR-Len:(1,0)
            , A.[XKKBI] COLLATE DATABASE_DEFAULT AS [XKKBI] --  -Indicador: ¿Se puede sobrescribir el área d/ctrl.de crédito?-Check: -Datatype:CHAR-Len:(1,0)
            , A.[WT_NEWWT] COLLATE DATABASE_DEFAULT AS [WT_NEWWT] --  -Indicador: liquidación de impuestos ampliada activa-Check: -Datatype:CHAR-Len:(1,0)
            , A.[PP_PDATE] COLLATE DATABASE_DEFAULT AS [PP_PDATE] --  -Procedimiento: especificar fecha de contabilización-Check: -Datatype:CHAR-Len:(1,0)
            , A.[INFMT] COLLATE DATABASE_DEFAULT AS [INFMT] --  -Método de inflación-Check:J_1AINFMET-Datatype:CHAR-Len:(4,0)
            , A.[FSTVARE] COLLATE DATABASE_DEFAULT AS [FSTVARE] --  -Variante status campos reserva recursos-Check:TREV-Datatype:CHAR-Len:(4,0)
            , A.[KOPIM] COLLATE DATABASE_DEFAULT AS [KOPIM] --  -Comercio exterior: control copia de datos importación p.EM-Check: -Datatype:CHAR-Len:(1,0)
            , A.[DKWEG] COLLATE DATABASE_DEFAULT AS [DKWEG] --  -Com.ext.: control imágenes de dat.import.p.entrada mcía.MM-Check: -Datatype:CHAR-Len:(1,0)
            , A.[OFFSACCT] COLLATE DATABASE_DEFAULT AS [OFFSACCT] --  -Método para determinación cuentas de contrapartida-Check: -Datatype:NUMC-Len:(1,0)
            , A.[BAPOVAR] COLLATE DATABASE_DEFAULT AS [BAPOVAR] --  -Variante de opción adicional para balances de división-Check:TGSB_CUS-Datatype:CHAR-Len:(2,0)
            , A.[XCOS] COLLATE DATABASE_DEFAULT AS [XCOS] --  -Status método de costes de ventas-Check: -Datatype:CHAR-Len:(1,0)
            , A.[XCESSION] COLLATE DATABASE_DEFAULT AS [XCESSION] --  -Operaciones de cesión activas-Check: -Datatype:CHAR-Len:(1,0)
            , A.[XSPLT] COLLATE DATABASE_DEFAULT AS [XSPLT] --  -Permitir partición imptes.-Check: -Datatype:CHAR-Len:(1,0)
            , A.[SURCCM] COLLATE DATABASE_DEFAULT AS [SURCCM] --  -Surcharge Calculation Method-Check: -Datatype:CHAR-Len:(1,0)
            , A.[DTPROV] COLLATE DATABASE_DEFAULT AS [DTPROV] --  -Document Type for Provisions for Taxes on Services Received-Check:T003-Datatype:CHAR-Len:(2,0)
            , A.[DTAMTC] COLLATE DATABASE_DEFAULT AS [DTAMTC] --  -Document Type for Journal Voucher (Amount Correction)-Check:T003-Datatype:CHAR-Len:(2,0)
            , A.[DTTAXC] COLLATE DATABASE_DEFAULT AS [DTTAXC] --  -Document Type for Journal Voucher (Tax Code Correction)-Check:T003-Datatype:CHAR-Len:(2,0)
            , A.[DTTDSP] COLLATE DATABASE_DEFAULT AS [DTTDSP] --  -Document Type for Remittance Challans-Check:T003-Datatype:CHAR-Len:(2,0)
            , A.[DTAXR] COLLATE DATABASE_DEFAULT AS [DTAXR] --  -Regla impuestos diferidos-Check:T007DT-Datatype:CHAR-Len:(4,0)
            , A.[XVATDATE] COLLATE DATABASE_DEFAULT AS [XVATDATE] --  -Fecha de declaración fiscal activa en documentos-Check: -Datatype:CHAR-Len:(1,0)
            , A.[PST_PER_VAR] COLLATE DATABASE_DEFAULT AS [PST_PER_VAR] --  -Manage Variant of Posting Period for Company Code/Ledger-Check: -Datatype:CHAR-Len:(1,0)
            , A.[FM_DERIVE_ACC] COLLATE DATABASE_DEFAULT AS [FM_DERIVE_ACC] --  -Activar derivación imputación en control presupuestario-Check: -Datatype:CHAR-Len:(1,0)
            , GETDATE() AS [Fecha_Carga]
            , GETDATE() AS [Fecha_Actualizado]
            INTO [DL_FARINTER].[dbo].[DL_SAP_T001]
            FROM SAPPRD.PRD.prd.T001 A WITH (NOLOCK)
            WHERE A.MANDT = '300';

            SET @filasactualizadas += @@ROWCOUNT;

            ALTER TABLE dbo.DL_SAP_T001
            DROP CONSTRAINT
            IF EXISTS PK_DL_SAP_T001
            
            ALTER TABLE dbo.DL_SAP_T001 ADD CONSTRAINT PK_DL_SAP_T001 PRIMARY KEY CLUSTERED (BUKRS) ON [PRIMARY]
        END;

		COMMIT TRANSACTION

		SET @resultado = 'paCargarDL_SAP_REPLICA_DatosMaestros.Finalizado T001: {last_date_updated} Filas:' + CAST(@filasactualizadas AS NVARCHAR)

		EXEC DL_paEscBitacora @resultado
			, 'Finalizado'
			, 'T001';
			--SELECT TOP 1000 * FROM [DL_FARINTER].[dbo].[DL_SAP_T001]
			--SELECT COUNT(*)  FROM [DL_FARINTER].[dbo].[DL_SAP_T001]
	END TRY

	BEGIN CATCH
		IF @@TRANCOUNT > 0
			ROLLBACK TRANSACTION

		SET @resultado = 'paCargarDL_SAP_REPLICA_DatosMaestros.Error T001: ' + ERROR_MESSAGE()

		EXEC DL_paEscBitacora @resultado
			, 'Error'
			, 'T001'
            ;
        THROW ;
	END CATCH
		--TABLA T001
END --T001
