import * as vscode from 'vscode';
import * as ui from './common/UI';
import { Session } from './common/Session';
import { AIHandler } from './chat/AIHandler';
import { CommandHistoryView } from './common/CommandHistoryView';
import { McpManager } from './mcp/McpManager';
import { McpManageView } from './mcp/McpManageView';
import { QueryFileTool } from './queryfile/QueryFileTool';

export function activate(context: vscode.ExtensionContext) {
	ui.logToOutput('Dataclaw is now active!');

	// Initialize Core Services
	const session = new Session(context);
	// Alway enable pro version for now
	// session.IsProVersion = isLicenseValid();
	
	new AIHandler();
	const mcpManager = new McpManager(context);

	// Register disposables
	context.subscriptions.push(
		session,
		mcpManager,
		{ dispose: () => ui.dispose() }
	);

	if (Session.Current?.IsHostSupportLanguageTools()) {
		context.subscriptions.push(
			vscode.lm.registerTool('dataclaw_QueryFileTool', new QueryFileTool())
		);
		ui.logToOutput('Registered QueryFileTool language model tool');
	}
	else {
		ui.logToOutput(`Language model tools registration skipped for ${Session.Current?.HostAppName}`);
	}

	ui.logToOutput('Language model tools registered');

	// Register Commands
	context.subscriptions.push(
		vscode.commands.registerCommand('dataclaw.AskAwsclaw', async () => { await AIHandler.Current.askAI(); }),



        vscode.commands.registerCommand('dataclaw.ShowCommandHistory', () => {
            if (!Session.Current) {
                ui.showErrorMessage('Session not initialized', new Error('No session'));
                return;
            }
            CommandHistoryView.Render(Session.Current.ExtensionUri);
        }),

 

		// vscode.commands.registerCommand('dataclaw.ActivatePro', () => {
		// 	if (Session.Current?.IsProVersion) {
		// 		ui.showInfoMessage('You already have an active Pro license!');
		// 		return;
		// 	}

		// 	let buyUrl = 'https://necatiarslan.lemonsqueezy.com/checkout/buy/077f6804-ab37-49b1-b8e4-1c63870d728f';
		// 	if (Session.Current?.IsDebugMode()) {
		// 		buyUrl = 'https://necatiarslan.lemonsqueezy.com/checkout/buy/ec1d3673-0b2a-423d-87f7-1822815bc665';
		// 	}

		// 	vscode.env.openExternal(vscode.Uri.parse(buyUrl));
		// 	vscode.commands.executeCommand('dataclaw.EnterLicenseKey');
		// }),

		// vscode.commands.registerCommand('dataclaw.EnterLicenseKey', async () => {
		// 	if (Session.Current?.IsProVersion) {
		// 		ui.showInfoMessage('You already have an active Pro license!');
		// 		return;
		// 	}

		// 	await promptForLicense(context);
		// 	if (Session.Current) {
		// 		Session.Current.IsProVersion = isLicenseValid();
		// 	}
		// }),

		// vscode.commands.registerCommand('dataclaw.ResetLicenseKey', async () => {
		// 	await clearLicense();
		// 	ui.showInfoMessage('License key has been reset. Please enter a new license key to activate Pro features.');
		// 	if (Session.Current) {
		// 		Session.Current.IsProVersion = false;
		// 	}
		// }),

		vscode.commands.registerCommand('dataclaw.StartMcpServer', async () => {
			if (!Session.Current) {
				ui.showErrorMessage('Session not initialized', new Error('No session'));
				return;
			}
			if(Session.Current.IsHostSupportLanguageTools()) {
				ui.showInfoMessage('MCP server is not required in VsCode');
				return;
			}
			await mcpManager.startSession();
		}),

		vscode.commands.registerCommand('dataclaw.StopMcpServers', () => {
			if(!Session.Current) { return; }
			if(Session.Current.IsHostSupportLanguageTools()) {
				ui.showInfoMessage('MCP server is not required in VsCode');
				return;
			}
			mcpManager.stopAll();
			ui.showInfoMessage('All MCP sessions stopped.');
		}),

		vscode.commands.registerCommand('dataclaw.OpenMcpManageView', () => {
			if(!Session.Current) { return; }
			if(Session.Current.IsHostSupportLanguageTools()) {
				ui.showInfoMessage('MCP server is not required in VsCode');
				return;
			}
			McpManageView.Render(context.extensionUri, mcpManager);
		}),

		vscode.commands.registerCommand('dataclaw.LoadMoreResults', async () => {
			// Reserved for future pagination support
		})
	);
}

export function deactivate() {
	ui.logToOutput('Dataclaw is now de-active!');
}

